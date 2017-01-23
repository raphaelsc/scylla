/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <vector>
#include <map>
#include <functional>
#include <utility>
#include <assert.h>
#include <algorithm>

#include <boost/range/algorithm.hpp>
#include <boost/range/adaptors.hpp>
#include <boost/range/join.hpp>

#include "core/future-util.hh"
#include "core/pipe.hh"

#include "sstables.hh"
#include "compaction.hh"
#include "database.hh"
#include "mutation_reader.hh"
#include "schema.hh"
#include "db/system_keyspace.hh"
#include "db/query_context.hh"
#include "service/storage_service.hh"
#include "service/priority_manager.hh"
#include "db_clock.hh"
#include "mutation_compactor.hh"
#include "leveled_manifest.hh"

namespace sstables {

logging::logger logger("compaction");

class sstable_reader final : public ::mutation_reader::impl {
    shared_sstable _sst;
    mutation_reader _reader;
public:
    sstable_reader(shared_sstable sst, schema_ptr schema)
            : _sst(std::move(sst))
            , _reader(_sst->read_rows(schema, service::get_local_compaction_priority()))
            {}
    virtual future<streamed_mutation_opt> operator()() override {
        return _reader.read().handle_exception([sst = _sst] (auto ep) {
            logger.error("Compaction found an exception when reading sstable {} : {}",
                    sst->get_filename(), ep);
            return make_exception_future<streamed_mutation_opt>(ep);
        });
    }
};

static api::timestamp_type get_max_purgeable_timestamp(const column_family& cf, sstable_set::incremental_selector& selector,
        const std::unordered_set<shared_sstable>& compacting_set, const dht::decorated_key& dk) {
    auto timestamp = api::max_timestamp;
    stdx::optional<utils::hashed_key> hk;
    for (auto&& sst : boost::range::join(selector.select(dk.token()), cf.compacted_undeleted_sstables())) {
        if (compacting_set.count(sst)) {
            continue;
        }
        if (!hk) {
            hk = sstables::sstable::make_hashed_key(*cf.schema(), dk.key());
        }
        if (sst->filter_has_key(*hk)) {
            timestamp = std::min(timestamp, sst->get_stats_metadata().min_timestamp);
        }
    }
    return timestamp;
}

static bool belongs_to_current_node(const dht::token& t, const dht::token_range_vector& sorted_owned_ranges) {
    auto low = std::lower_bound(sorted_owned_ranges.begin(), sorted_owned_ranges.end(), t,
            [] (const range<dht::token>& a, const dht::token& b) {
        // check that range a is before token b.
        return a.after(b, dht::token_comparator());
    });

    if (low != sorted_owned_ranges.end()) {
        const dht::token_range& r = *low;
        return r.contains(t, dht::token_comparator());
    }

    return false;
}

static void delete_sstables_for_interrupted_compaction(std::vector<shared_sstable>& new_sstables, sstring& ks, sstring& cf) {
    // Delete either partially or fully written sstables of a compaction that
    // was either stopped abruptly (e.g. out of disk space) or deliberately
    // (e.g. nodetool stop COMPACTION).
    for (auto& sst : new_sstables) {
        logger.debug("Deleting sstable {} of interrupted compaction for {}.{}", sst->get_filename(), ks, cf);
        sst->mark_for_deletion();
    }
}

static std::vector<shared_sstable> get_uncompacting_sstables(column_family& cf, std::vector<shared_sstable>& sstables) {
    auto all_sstables = boost::copy_range<std::vector<shared_sstable>>(*cf.get_sstables_including_compacted_undeleted());
    boost::sort(all_sstables, [] (const shared_sstable& x, const shared_sstable& y) {
        return x->generation() < y->generation();
    });
    std::sort(sstables.begin(), sstables.end(), [] (const shared_sstable& x, const shared_sstable& y) {
        return x->generation() < y->generation();
    });
    std::vector<shared_sstable> not_compacted_sstables;
    boost::set_difference(all_sstables, sstables,
        std::back_inserter(not_compacted_sstables), [] (const shared_sstable& x, const shared_sstable& y) {
            return x->generation() < y->generation();
        });
    return not_compacted_sstables;
}

class compacting_sstable_writer {
    const schema& _schema;
    std::function<shared_sstable(shard_id)> _creator;
    uint64_t _partitions_per_sstable;
    uint64_t _max_sstable_size;
    uint32_t _sstable_level;
    db::replay_position _rp;
    std::vector<unsigned long> _ancestors;
    compaction_info& _info;
    // multiple writer is supported for compaction that will generate one sstable
    // for every shard owning the input sstables.
    std::vector<std::pair<shared_sstable, stdx::optional<sstable_writer>>> _sstables;
    shard_id _current_shard;
private:
    void finish_sstable_write(shared_sstable& sst, stdx::optional<sstable_writer>& writer) {
        writer->consume_end_of_stream();
        writer = stdx::nullopt;

        sst->open_data().get0();
        _info.end_size += sst->data_size();
    }

    stdx::optional<sstable_writer>& current_writer() {
        return _sstables[_current_shard].second;
    }
public:
    compacting_sstable_writer(const schema& s, std::function<shared_sstable(shard_id)> creator, uint64_t partitions_per_sstable,
                              uint64_t max_sstable_size, uint32_t sstable_level, db::replay_position rp,
                              std::vector<unsigned long> ancestors, compaction_info& info)
        : _schema(s)
        , _creator(creator)
        , _partitions_per_sstable(partitions_per_sstable)
        , _max_sstable_size(max_sstable_size)
        , _sstable_level(sstable_level)
        , _rp(rp)
        , _ancestors(std::move(ancestors))
        , _info(info)
        , _sstables(smp::count) // TODO: maybe turn smp::count into a parameter (int max_shards=1) to avoid extra space and make writer resilient.
    { }

    void consume_new_partition(const dht::decorated_key& dk) {
        if (_info.is_stop_requested()) {
            // Compaction manager will catch this exception and re-schedule the compaction.
            throw compaction_stop_exception(_info.ks, _info.cf, _info.stop_requested);
        }

        auto shard = dht::shard_of(dk.token());
        auto& sst = _sstables[shard].first;
        auto& writer = _sstables[shard].second;
        if (!writer) {
            sst = _creator(shard);
            _info.new_sstables.push_back(sst);
            sst->get_metadata_collector().set_replay_position(_rp);
            sst->get_metadata_collector().sstable_level(_sstable_level);
            for (auto ancestor : _ancestors) { // find boost function to do it
                sst->add_ancestor(ancestor);
            }

            auto&& priority = service::get_local_compaction_priority();
            writer.emplace(sst->get_writer(_schema, _partitions_per_sstable, _max_sstable_size, false, priority));
        }
        _current_shard = shard;
        _info.total_keys_written++;
        writer->consume_new_partition(dk);
    }

    void consume(tombstone t) { current_writer()->consume(t); }
    stop_iteration consume(static_row&& sr, tombstone, bool) { return current_writer()->consume(std::move(sr)); }
    stop_iteration consume(clustering_row&& cr, tombstone, bool) { return current_writer()->consume(std::move(cr)); }
    stop_iteration consume(range_tombstone&& rt) { return current_writer()->consume(std::move(rt)); }

    stop_iteration consume_end_of_partition() {
        auto ret = current_writer()->consume_end_of_partition();
        if (ret == stop_iteration::yes) {
            finish_sstable_write(_sstables[_current_shard].first, _sstables[_current_shard].second);
        }
        return ret;
    }

    void consume_end_of_stream() {
        for (auto& p : _sstables) {
            if (p.second) {
                finish_sstable_write(p.first, p.second);
            }
        }
    }
};

class compaction_base {
protected:
    column_family& _cf;
    std::vector<shared_sstable> _sstables;
    std::function<shared_sstable()> _creator;
    uint64_t _max_sstable_size;
    uint32_t _sstable_level;
    const sstable_set _set;
    sstable_set::incremental_selector _selector;
    lw_shared_ptr<compaction_info> _info = make_lw_shared<compaction_info>();
    std::vector<::mutation_reader> _readers;
    uint64_t _estimated_partitions = 0;
    std::vector<unsigned long> _ancestors;
    sstring _formatted_msg;
    db::replay_position _rp;
protected:
    compaction_base(column_family& cf, std::vector<shared_sstable> sstables, std::function<shared_sstable()> creator,
            uint64_t max_sstable_size, uint32_t sstable_level)
        : _cf(cf)
        , _sstables(std::move(sstables))
        , _creator (std::move(creator))
        , _max_sstable_size(max_sstable_size)
        , _sstable_level(sstable_level)
        , _set(cf.get_sstable_set())
        , _selector(_set.make_incremental_selector())
    {
        _cf.get_compaction_manager().register_compaction(_info);
    }

    uint64_t partitions_per_sstable() const {
        uint64_t estimated_sstables = std::max(1UL, uint64_t(ceil(double(_info->start_size) / _max_sstable_size)));
        return ceil(double(_estimated_partitions) / estimated_sstables);
    }

    static int64_t calculate_generation_for_new_table(column_family& cf) {
        return cf.calculate_generation_for_new_table();
    }
    static sstring datadir(column_family& cf) {
        return cf._config.datadir;
    }
public:
    compaction_base& operator=(const compaction_base&) = delete;
    compaction_base(const compaction_base&) = delete;

    virtual ~compaction_base() {
        _cf.get_compaction_manager().deregister_compaction(_info);
    }

    virtual void setup() {
        auto schema = _cf.schema();
        _formatted_msg = "[";
        for (auto& sst : _sstables) {
            // We also capture the sstable, so we keep it alive while the read isn't done
            _readers.emplace_back(make_mutation_reader<sstable_reader>(sst, schema));
            // FIXME: If the sstables have cardinality estimation bitmaps, use that
            // for a better estimate for the number of partitions in the merged
            // sstable than just adding up the lengths of individual sstables.
            _estimated_partitions += sst->get_estimated_key_count();
            _info->total_partitions += sst->get_estimated_key_count();
            // Compacted sstable keeps track of its ancestors.
            _ancestors.push_back(sst->generation());
            _formatted_msg += sprint("%s:level=%d, ", sst->get_filename(), sst->get_sstable_level());
            _info->start_size += sst->data_size();
            // TODO:
            // Note that this is not fully correct. Since we might be merging sstables that originated on
            // another shard (#cpu changed), we might be comparing RP:s with differing shard ids,
            // which might vary in "comparable" size quite a bit. However, since the worst that happens
            // is that we might miss a high water mark for the commit log replayer,
            // this is kind of ok, esp. since we will hopefully not be trying to recover based on
            // compacted sstables anyway (CL should be clean by then).
            _rp = std::max(_rp, sst->get_stats_metadata().position);
        }
        _formatted_msg += "]";
        _info->sstables = _sstables.size();
        _info->ks = schema->ks_name();
        _info->cf = schema->cf_name();
        _info->type = compaction_type::Compaction;
    }

    virtual void finish(std::chrono::time_point<db_clock> started_at, std::chrono::time_point<db_clock> ended_at) {
        auto ratio = double(_info->end_size) / double(_info->start_size);
        auto duration = std::chrono::duration<float>(ended_at - started_at);
        auto throughput = (double(_info->end_size) / (1024*1024)) / duration.count();
        sstring new_sstables_msg;
        for (auto& newtab : _info->new_sstables) {
            new_sstables_msg += sprint("%s:level=%d, ", newtab->get_filename(), newtab->get_sstable_level());
        }

        // FIXME: there is some missing information in the log message below.
        // look at CompactionTask::runMayThrow() in origin for reference.
        // - add support to merge summary (message: Partition merge counts were {%s}.).
        // - there is no easy way, currently, to know the exact number of total partitions.
        // By the time being, using estimated key count.
        _formatted_msg = sprint("%ld sstables to [%s]. %ld bytes to %ld (~%d%% of original) in %dms = %.2fMB/s. " \
            "~%ld total partitions merged to %ld.",
            _info->sstables, new_sstables_msg, _info->start_size, _info->end_size, int(ratio * 100),
            std::chrono::duration_cast<std::chrono::milliseconds>(duration).count(), throughput,
            _info->total_partitions, _info->total_keys_written);
    }

    virtual std::function<api::timestamp_type(const dht::decorated_key&)> max_purgeable_func() {
        std::unordered_set<shared_sstable> compacting(_sstables.begin(), _sstables.end());
        return [this, compacting = std::move(compacting)] (const dht::decorated_key& dk) {
            return get_max_purgeable_timestamp(_cf, _selector, compacting, dk);
        };
    }

    virtual std::function<bool(const streamed_mutation& sm)> filter_func() {
        return [] (const streamed_mutation& sm) {
            return true;
        };
    }

    // default impl asks local column family for generating a sstable, but it may
    // be extended for owner shard to calculate generation for new sstable.
    virtual compacting_sstable_writer get_compacting_sstable_writer() const {
        auto creat = [this] (shard_id shard) {
            return _creator();
        };
        return compacting_sstable_writer(*_cf.schema(), creat, partitions_per_sstable(),
            _max_sstable_size, _sstable_level, _rp, _ancestors, *_info);
    }

    const schema_ptr& schema() const {
        return _cf.schema();
    }

    ::mutation_reader make_combined_reader() {
        return ::make_combined_reader(std::move(_readers));
    }

    lw_shared_ptr<compaction_info>& info() {
        return _info;
    }
};

class regular_compaction final : public compaction_base {
public:
    regular_compaction(column_family& cf, std::vector<shared_sstable> sstables, std::function<shared_sstable()> creator,
            uint64_t max_sstable_size, uint32_t sstable_level)
        : compaction_base(cf, std::move(sstables), std::move(creator), max_sstable_size, sstable_level)
    {
    }

    void setup() override {
        compaction_base::setup();
        logger.info("Compacting {}", _formatted_msg);
    }

    void finish(std::chrono::time_point<db_clock> started_at, std::chrono::time_point<db_clock> ended_at) override {
        compaction_base::finish(std::move(started_at), ended_at);
        logger.info("Compacted {}", _formatted_msg);

        // skip update if running without a query context, for example, when running a test case.
        if (!db::qctx) {
            return;
        }
        // FIXME: add support to merged_rows. merged_rows is a histogram that
        // shows how many sstables each row is merged from. This information
        // cannot be accessed until we make combined_reader more generic,
        // for example, by adding a reducer method.
        auto compacted_at = std::chrono::duration_cast<std::chrono::milliseconds>(ended_at.time_since_epoch()).count();
        db::system_keyspace::update_compaction_history(_info->ks, _info->cf, compacted_at,
            _info->start_size, _info->end_size, std::unordered_map<int32_t, int64_t>{}).get0();
    }

    std::function<bool(const streamed_mutation& sm)> filter_func() override {
        return [] (const streamed_mutation& sm) {
            return dht::shard_of(sm.decorated_key().token()) == engine().cpu_id();
        };
    }
};

class cleanup_compaction final : public compaction_base {
public:
    cleanup_compaction(column_family& cf, std::vector<shared_sstable> sstables, std::function<shared_sstable()> creator,
            uint64_t max_sstable_size, uint32_t sstable_level)
        : compaction_base(cf, std::move(sstables), std::move(creator), max_sstable_size, sstable_level)
    {
        _info->type = compaction_type::Cleanup;
    }

    void setup() override {
        compaction_base::setup();
        logger.info("Cleaning {}", _formatted_msg);
    }

    void finish(std::chrono::time_point<db_clock> started_at, std::chrono::time_point<db_clock> ended_at) override {
        compaction_base::finish(std::move(started_at), std::move(ended_at));
        logger.info("Cleaned {}", _formatted_msg);
    }

    std::function<bool(const streamed_mutation& sm)> filter_func() override {
        dht::token_range_vector owned_ranges = service::get_local_storage_service().get_local_ranges(_cf.schema()->ks_name());

        return [this, owned_ranges = std::move(owned_ranges)] (const streamed_mutation& sm) {
            if (dht::shard_of(sm.decorated_key().token()) != engine().cpu_id()) {
                return false;
            }
            if (!belongs_to_current_node(sm.decorated_key().token(), owned_ranges)) {
                return false;
            }
            return true;
        };
    }
};

class resharding_compaction final : public compaction_base {
    distributed<database>& _db;
    sstring _ks_name;
    sstring _cf_name;
public:
    resharding_compaction(distributed<database>& db, std::vector<shared_sstable> sstables, sstring ks_name, sstring cf_name,
            uint64_t max_sstable_size, uint32_t sstable_level)
        // TODO: probably use a different constructor to avoid setting _set and _selector.
        : compaction_base(db.local().find_column_family(ks_name, cf_name), std::move(sstables), {}, max_sstable_size, sstable_level)
        , _db(db)
        , _ks_name(std::move(ks_name))
        , _cf_name(std::move(cf_name))
    {
    }

    void setup() override {
        compaction_base::setup();
        logger.info("Resharding {}", _formatted_msg);
    }

    void finish(std::chrono::time_point<db_clock> started_at, std::chrono::time_point<db_clock> ended_at) override {
        compaction_base::finish(std::move(started_at), std::move(ended_at));
        logger.info("Resharded {}", _formatted_msg);
    }

    std::function<api::timestamp_type(const dht::decorated_key&)> max_purgeable_func() override {
        // TODO: Maybe, add support for purging tombstones while resharding.
        // That requires creating a set with sstable set from column family in every shard.
        return [] (const dht::decorated_key& dk) {
            return api::min_timestamp;
        };
    }

    compacting_sstable_writer get_compacting_sstable_writer() const override {
        auto creat = [this] (shard_id shard) mutable {
            // we need generation calculated by instance of cf at requested shard,
            // or resource usage wouldn't be fairly distributed among shards.
            auto gen = _db.invoke_on(shard, [ks_name = _ks_name, cf_name = _cf_name] (auto& db) {
                auto& cf = db.find_column_family(ks_name, cf_name);
                return calculate_generation_for_new_table(cf);
            }).get0();

            auto sst = make_lw_shared<sstables::sstable>(_cf.schema(), datadir(_cf), gen,
                sstables::sstable::version_types::ka, sstables::sstable::format_types::big);
            sst->set_unshared();
            return sst;
        };
        return compacting_sstable_writer(*_cf.schema(), creat, partitions_per_sstable(),
            _max_sstable_size, _sstable_level, _rp, _ancestors, *_info);
    }
};

static std::unique_ptr<compaction_base>
make_compaction_base(column_family& cf, std::vector<shared_sstable> sstables, std::function<shared_sstable()> creator,
        uint64_t max_sstable_size, uint32_t sstable_level, bool cleanup) {
    std::unique_ptr<compaction_base> c;
    if (!cleanup) {
        c = std::make_unique<regular_compaction>(cf, std::move(sstables), std::move(creator), max_sstable_size, sstable_level);
    } else {
        c = std::make_unique<cleanup_compaction>(cf, std::move(sstables), std::move(creator), max_sstable_size, sstable_level);
    }
    return c;
}

static future<std::vector<shared_sstable>> compact(std::unique_ptr<compaction_base> c) {
    return seastar::async([c = std::move(c)] () mutable {
        c->setup();

        auto cr = c->get_compacting_sstable_writer();
        auto cfc = make_stable_flattened_mutations_consumer<compact_for_compaction<compacting_sstable_writer>>(
            *c->schema(), gc_clock::now(), std::move(cr), c->max_purgeable_func());

        auto reader = c->make_combined_reader();
        auto start_time = db_clock::now();
        try {
            consume_flattened_in_thread(reader, cfc, c->filter_func());
        } catch (...) {
            auto info = c->info();
            delete_sstables_for_interrupted_compaction(info->new_sstables, info->ks, info->cf);
            throw;
        }

        c->finish(std::move(start_time), db_clock::now());

        return std::move(c->info()->new_sstables);
    });
}

future<std::vector<shared_sstable>>
compact_sstables(std::vector<shared_sstable> sstables, column_family& cf, std::function<shared_sstable()> creator,
        uint64_t max_sstable_size, uint32_t sstable_level, bool cleanup) {
    assert(sstables.size() > 0);
    auto c = make_compaction_base(cf, std::move(sstables), std::move(creator), max_sstable_size, sstable_level, cleanup);
    return compact(std::move(c));
}

future<std::vector<shared_sstable>>
reshard_sstables(distributed<database>& db, std::vector<shared_sstable> sstables, sstring ks_name, sstring cf_name,
        uint64_t max_sstable_size, uint32_t sstable_level) {
    assert(sstables.size() > 0);
    auto c = std::make_unique<resharding_compaction>(db, std::move(sstables), std::move(ks_name), std::move(cf_name), max_sstable_size, sstable_level);
    return compact(std::move(c));
}

std::vector<sstables::shared_sstable>
get_fully_expired_sstables(column_family& cf, std::vector<sstables::shared_sstable>& compacting, int32_t gc_before) {
    logger.debug("Checking droppable sstables in {}.{}", cf.schema()->ks_name(), cf.schema()->cf_name());

    if (compacting.empty()) {
        return {};
    }

    std::list<sstables::shared_sstable> candidates;
    auto uncompacting_sstables = get_uncompacting_sstables(cf, compacting);
    // Get list of uncompacting sstables that overlap the ones being compacted.
    std::vector<sstables::shared_sstable> overlapping = leveled_manifest::overlapping(*cf.schema(), compacting, uncompacting_sstables);
    int64_t min_timestamp = std::numeric_limits<int64_t>::max();

    for (auto& sstable : overlapping) {
        if (sstable->get_stats_metadata().max_local_deletion_time >= gc_before) {
            min_timestamp = std::min(min_timestamp, sstable->get_stats_metadata().min_timestamp);
        }
    }

    // SStables that do not contain live data is added to list of possibly expired sstables.
    for (auto& candidate : compacting) {
        logger.debug("Checking if candidate of generation {} and max_deletion_time {} is expired, gc_before is {}",
                    candidate->generation(), candidate->get_stats_metadata().max_local_deletion_time, gc_before);
        if (candidate->get_stats_metadata().max_local_deletion_time < gc_before) {
            logger.debug("Adding candidate of generation {} to list of possibly expired sstables", candidate->generation());
            candidates.push_back(candidate);
        } else {
            min_timestamp = std::min(min_timestamp, candidate->get_stats_metadata().min_timestamp);
        }
    }

    auto it = candidates.begin();
    while (it != candidates.end()) {
        auto& candidate = *it;
        // Remove from list any candidate that may contain a tombstone that covers older data.
        if (candidate->get_stats_metadata().max_timestamp >= min_timestamp) {
            it = candidates.erase(it);
        } else {
            logger.debug("Dropping expired SSTable {} (maxLocalDeletionTime={}, gcBefore={})",
                    candidate->get_filename(), candidate->get_stats_metadata().max_local_deletion_time, gc_before);
            it++;
        }
    }
    return std::vector<sstables::shared_sstable>(candidates.begin(), candidates.end());
}

}
