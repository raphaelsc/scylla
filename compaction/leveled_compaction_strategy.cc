/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "leveled_compaction_strategy.hh"
#include "leveled_manifest.hh"
#include <algorithm>

#include <boost/range/algorithm/remove_if.hpp>

namespace sstables {

compaction_descriptor leveled_compaction_strategy::get_sstables_for_compaction(table_state& table_s, strategy_control& control, std::vector<sstables::shared_sstable> candidates) {
    // NOTE: leveled_manifest creation may be slightly expensive, so later on,
    // we may want to store it in the strategy itself. However, the sstable
    // lists managed by the manifest may become outdated. For example, one
    // sstable in it may be marked for deletion after compacted.
    // Currently, we create a new manifest whenever it's time for compaction.
    leveled_manifest manifest = leveled_manifest::create(table_s, candidates, _max_sstable_size_in_mb, _stcs_options);
    if (!_last_compacted_keys) {
        generate_last_compacted_keys(manifest);
    }
    auto candidate = manifest.get_compaction_candidates(*_last_compacted_keys, _compaction_counter);

    if (!candidate.sstables.empty()) {
        leveled_manifest::logger.debug("leveled: Compacting {} out of {} sstables", candidate.sstables.size(), table_s.get_sstable_set().all()->size());
        return candidate;
    }

    // if there is no sstable to compact in standard way, try compacting based on droppable tombstone ratio
    // unlike stcs, lcs can look for sstable with highest droppable tombstone ratio, so as not to choose
    // a sstable which droppable data shadow data in older sstable, by starting from highest levels which
    // theoretically contain oldest non-overlapping data.
    auto compaction_time = gc_clock::now();
    for (auto level = int(manifest.get_level_count()); level >= 0; level--) {
        auto& sstables = manifest.get_level(level);
        // filter out sstables which droppable tombstone ratio isn't greater than the defined threshold.
        auto e = boost::range::remove_if(sstables, [this, compaction_time] (const sstables::shared_sstable& sst) -> bool {
            return !worth_dropping_tombstones(sst, compaction_time);
        });
        sstables.erase(e, sstables.end());
        if (sstables.empty()) {
            continue;
        }
        auto& sst = *std::max_element(sstables.begin(), sstables.end(), [&] (auto& i, auto& j) {
            auto gc_before1 = i->get_gc_before_for_drop_estimation(compaction_time);
            auto gc_before2 = j->get_gc_before_for_drop_estimation(compaction_time);
            return i->estimate_droppable_tombstone_ratio(gc_before1) < j->estimate_droppable_tombstone_ratio(gc_before2);
        });
        return sstables::compaction_descriptor({ sst }, service::get_local_compaction_priority(), sst->get_sstable_level());
    }
    return {};
}

compaction_descriptor leveled_compaction_strategy::get_major_compaction_job(table_state& table_s, std::vector<sstables::shared_sstable> candidates) {
    if (candidates.empty()) {
        return compaction_descriptor();
    }

    auto max_sstable_size_in_bytes = _max_sstable_size_in_mb*1024*1024;
    auto ideal_level = ideal_level_for_input(candidates, max_sstable_size_in_bytes);
    return compaction_descriptor(std::move(candidates), service::get_local_compaction_priority(),
                                 ideal_level, max_sstable_size_in_bytes);
}

void leveled_compaction_strategy::notify_completion(const std::vector<shared_sstable>& removed, const std::vector<shared_sstable>& added) {
    if (removed.empty() || added.empty()) {
        return;
    }
    auto min_level = std::numeric_limits<uint32_t>::max();
    for (auto& sstable : removed) {
        min_level = std::min(min_level, sstable->get_sstable_level());
    }

    const sstables::sstable *last = nullptr;
    int target_level = 0;
    for (auto& candidate : added) {
        if (!last || last->compare_by_first_key(*candidate) < 0) {
            last = &*candidate;
        }
        target_level = std::max(target_level, int(candidate->get_sstable_level()));
    }
    _last_compacted_keys.value().at(min_level) = last->get_last_decorated_key();

    for (int i = leveled_manifest::MAX_LEVELS - 1; i > 0; i--) {
        _compaction_counter[i]++;
    }
    _compaction_counter[target_level] = 0;

    if (leveled_manifest::logger.level() == logging::log_level::debug) {
        for (auto j = 0U; j < _compaction_counter.size(); j++) {
            leveled_manifest::logger.debug("CompactionCounter: {}: {}", j, _compaction_counter[j]);
        }
    }
}

void leveled_compaction_strategy::generate_last_compacted_keys(leveled_manifest& manifest) {
    std::vector<std::optional<dht::decorated_key>> last_compacted_keys(leveled_manifest::MAX_LEVELS);
    for (auto i = 0; i < leveled_manifest::MAX_LEVELS - 1; i++) {
        if (manifest.get_level(i + 1).empty()) {
            continue;
        }

        const sstables::sstable* sstable_with_last_compacted_key = nullptr;
        std::optional<db_clock::time_point> max_creation_time;
        for (auto& sst : manifest.get_level(i + 1)) {
            auto wtime = sst->data_file_write_time();
            if (!max_creation_time || wtime >= *max_creation_time) {
                sstable_with_last_compacted_key = &*sst;
                max_creation_time = wtime;
            }
        }
        last_compacted_keys[i] = sstable_with_last_compacted_key->get_last_decorated_key();
    }
    _last_compacted_keys = std::move(last_compacted_keys);
}

int64_t leveled_compaction_strategy::estimated_pending_compactions(table_state& table_s) const {
    std::vector<sstables::shared_sstable> sstables;
    auto all_sstables = table_s.get_sstable_set().all();
    sstables.reserve(all_sstables->size());
    for (auto& entry : *all_sstables) {
        sstables.push_back(entry);
    }
    return leveled_manifest::get_estimated_tasks(leveled_manifest::get_levels(sstables), _max_sstable_size_in_mb * 1024 * 1024);
}

compaction_descriptor
leveled_compaction_strategy::get_reshaping_job(std::vector<shared_sstable> input, schema_ptr schema, const ::io_priority_class& iop, reshape_mode mode) {
    std::array<std::vector<shared_sstable>, leveled_manifest::MAX_LEVELS> level_info;

    auto is_disjoint = [this, schema] (const std::vector<shared_sstable>& sstables, unsigned tolerance) -> std::tuple<bool, unsigned> {
        auto overlapping_sstables = sstable_set_overlapping_count(schema, sstables);
        return { overlapping_sstables <= tolerance, overlapping_sstables };
    };

    auto max_sstable_size_in_bytes = _max_sstable_size_in_mb * 1024 * 1024;

    for (auto& sst : input) {
        auto sst_level = sst->get_sstable_level();
        if (sst_level > leveled_manifest::MAX_LEVELS - 1) {
            leveled_manifest::logger.warn("Found SSTable with level {}, higher than the maximum {}. This is unexpected, but will fix", sst_level, leveled_manifest::MAX_LEVELS - 1);

            // This is really unexpected, so we'll just compact it all to fix it
            auto ideal_level = ideal_level_for_input(input, max_sstable_size_in_bytes);
            compaction_descriptor desc(std::move(input), iop, ideal_level, max_sstable_size_in_bytes);
            desc.options = compaction_type_options::make_reshape();
            return desc;
        }
        level_info[sst_level].push_back(sst);
    }

    // Can't use std::ranges::views::drop due to https://bugs.llvm.org/show_bug.cgi?id=47509
    for (auto i = level_info.begin(); i != level_info.end(); ++i) {
        auto& level = *i;
        std::sort(level.begin(), level.end(), [&schema] (const shared_sstable& a, const shared_sstable& b) {
            return dht::ring_position(a->get_first_decorated_key()).less_compare(*schema, dht::ring_position(b->get_first_decorated_key()));
        });
    }

    size_t offstrategy_threshold = (mode == reshape_mode::strict) ? std::max(schema->min_compaction_threshold(), 4) : std::max(schema->max_compaction_threshold(), 32);
    size_t max_sstables = std::max(schema->max_compaction_threshold(), int(offstrategy_threshold));
    auto tolerance = [mode] (unsigned level) -> unsigned {
        if (mode == reshape_mode::strict) {
            return 0;
        }
        constexpr unsigned fan_out = leveled_manifest::leveled_fan_out;
        return std::max(double(fan_out), std::ceil(std::pow(fan_out, level) * 0.1));
    };

    // If there's only disjoint L0 sstables like on bootstrap, let's compact them all into a level L which has capacity to store the output.
    // The best possible level can be calculated with the formula: log (base fan_out) of (L0_total_bytes / max_sstable_size)
    auto [l0_disjoint, _] = is_disjoint(level_info[0], 0);
    if (mode == reshape_mode::strict && level_info[0].size() >= offstrategy_threshold && level_info[0].size() == input.size() && l0_disjoint) {
        unsigned ideal_level = ideal_level_for_input(level_info[0], max_sstable_size_in_bytes);

        leveled_manifest::logger.info("Reshaping {} disjoint sstables in level 0 into level {}", level_info[0].size(), ideal_level);
        compaction_descriptor desc(std::move(input), iop, ideal_level, max_sstable_size_in_bytes);
        desc.options = compaction_type_options::make_reshape();
        return desc;
    }

    if (level_info[0].size() > offstrategy_threshold) {
        size_tiered_compaction_strategy stcs(_stcs_options);
        return stcs.get_reshaping_job(std::move(level_info[0]), schema, iop, mode);
    }

    for (unsigned level = leveled_manifest::MAX_LEVELS - 1; level > 0; --level) {
        if (level_info[level].empty()) {
            continue;
        }

        auto [disjoint, overlapping_sstables] = is_disjoint(level_info[level], tolerance(level));
        if (!disjoint) {
            auto ideal_level = ideal_level_for_input(input, max_sstable_size_in_bytes);
            leveled_manifest::logger.warn("Turns out that level {} is not disjoint, found {} overlapping SSTables, so compacting everything on behalf of {}.{}", level, overlapping_sstables, schema->ks_name(), schema->cf_name());
            // Unfortunately no good limit to limit input size to max_sstables for LCS major
            compaction_descriptor desc(std::move(input), iop, ideal_level, max_sstable_size_in_bytes);
            desc.options = compaction_type_options::make_reshape();
            return desc;
        }
    }

   return compaction_descriptor();
}

std::vector<compaction_descriptor>
leveled_compaction_strategy::get_cleanup_compaction_jobs(table_state& table_s, std::vector<shared_sstable> candidates) const {
    std::vector<compaction_descriptor> ret;

    auto levels = leveled_manifest::get_levels(candidates);

    ret = size_tiered_compaction_strategy(_stcs_options).get_cleanup_compaction_jobs(table_s, std::move(levels[0]));
    for (auto level = 1; level < levels.size(); level++) {
        if (levels[level].empty()) {
            continue;
        }
        ret.push_back(compaction_descriptor(std::move(levels[level]), service::get_local_compaction_priority(), level, _max_sstable_size_in_mb * 1024 * 1024));
    }
    return ret;
}

unsigned leveled_compaction_strategy::ideal_level_for_input(const std::vector<sstables::shared_sstable>& input, uint64_t max_sstable_size) {
    auto log_fanout = [fanout = leveled_manifest::leveled_fan_out] (double x) {
        double inv_log_fanout = 1.0f / std::log(fanout);
        return log(x) * inv_log_fanout;
    };
    uint64_t total_bytes = std::max(leveled_manifest::get_total_bytes(input), max_sstable_size);
    return std::ceil(log_fanout(total_bytes / max_sstable_size));
}

}
