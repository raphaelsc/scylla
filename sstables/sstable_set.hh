/*
 * Copyright (C) 2016 ScyllaDB
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

#pragma once

#include "flat_mutation_reader.hh"
#include "mutation_reader.hh"
#include "sstables/progress_monitor.hh"
#include "shared_sstable.hh"
#include "dht/i_partitioner.hh"
#include <seastar/core/shared_ptr.hh>
#include <vector>
#include <boost/range.hpp>
#include <boost/range/join.hpp>

namespace utils {
class estimated_histogram;
}

namespace sstables {

class sstable_set_impl;
class incremental_selector_impl;

// Structure holds all sstables (a.k.a. fragments) that belong to same run identifier, which is an UUID.
// SStables in that same run will not overlap with one another.
class sstable_run {
    sstable_list _all;
public:
    void insert(shared_sstable sst);
    void erase(shared_sstable sst);
    // Data size of the whole run, meaning it's a sum of the data size of all its fragments.
    uint64_t data_size() const;
    const sstable_list& all() const { return _all; }
};

class sstable_set : public enable_lw_shared_from_this<sstable_set> {
    std::unique_ptr<sstable_set_impl> _impl;
    schema_ptr _schema;
    // used to support column_family::get_sstable(), which wants to return an sstable_list
    // that has a reference somewhere
    lw_shared_ptr<sstable_list> _all;
    std::unordered_map<utils::UUID, sstable_run> _all_runs;
public:
    ~sstable_set();
    sstable_set(std::unique_ptr<sstable_set_impl> impl, schema_ptr s, lw_shared_ptr<sstable_list> all);
    sstable_set(const sstable_set&);
    sstable_set(sstable_set&&) noexcept;
    sstable_set& operator=(const sstable_set&);
    sstable_set& operator=(sstable_set&&) noexcept;
    std::vector<shared_sstable> select(const dht::partition_range& range) const;
    // Return all runs which contain any of the input sstables.
    std::vector<sstable_run> select_sstable_runs(const std::vector<shared_sstable>& sstables) const;
    lw_shared_ptr<sstable_list> all() const { return _all; }
    void insert(shared_sstable sst);
    void erase(shared_sstable sst);

    // Used to incrementally select sstables from sstable set using ring-position.
    // sstable set must be alive during the lifetime of the selector.
    class incremental_selector {
        std::unique_ptr<incremental_selector_impl> _impl;
        dht::ring_position_comparator _cmp;
        mutable std::optional<dht::partition_range> _current_range;
        mutable std::optional<nonwrapping_range<dht::ring_position_view>> _current_range_view;
        mutable std::vector<shared_sstable> _current_sstables;
        mutable dht::ring_position_view _current_next_position = dht::ring_position_view::min();
    public:
        ~incremental_selector();
        incremental_selector(std::unique_ptr<incremental_selector_impl> impl, const schema& s);
        incremental_selector(incremental_selector&&) noexcept;

        struct selection {
            const std::vector<shared_sstable>& sstables;
            dht::ring_position_view next_position;
        };

        // Return the sstables that intersect with `pos` and the next
        // position where the intersecting sstables change.
        // To walk through the token range incrementally call `select()`
        // with `dht::ring_position_view::min()` and then pass back the
        // returned `next_position` on each next call until
        // `next_position` becomes `dht::ring_position::max()`.
        //
        // Successive calls to `select()' have to pass weakly monotonic
        // positions (incrementability).
        //
        // NOTE: both `selection.sstables` and `selection.next_position`
        // are only guaranteed to be valid until the next call to
        // `select()`.
        selection select(const dht::ring_position_view& pos) const;
    };
    incremental_selector make_incremental_selector() const;

    flat_mutation_reader create_single_key_sstable_reader(
        column_family*,
        schema_ptr,
        reader_permit,
        utils::estimated_histogram&,
        const dht::partition_range&, // must be singular and contain a key
        const query::partition_slice&,
        const io_priority_class&,
        tracing::trace_state_ptr,
        streamed_mutation::forwarding,
        mutation_reader::forwarding) const;

    /// Read a range from the sstable set.
    ///
    /// The reader is unrestricted, but will account its resource usage on the
    /// semaphore belonging to the passed-in permit.
    flat_mutation_reader make_range_sstable_reader(
        schema_ptr,
        reader_permit,
        const dht::partition_range&,
        const query::partition_slice&,
        const io_priority_class&,
        tracing::trace_state_ptr,
        streamed_mutation::forwarding,
        mutation_reader::forwarding,
        read_monitor_generator& rmg = default_read_monitor_generator()) const;

    // Filters out mutations that don't belong to the current shard.
    flat_mutation_reader make_local_shard_sstable_reader(
        schema_ptr,
        reader_permit,
        const dht::partition_range&,
        const query::partition_slice&,
        const io_priority_class&,
        tracing::trace_state_ptr,
        streamed_mutation::forwarding,
        mutation_reader::forwarding,
        read_monitor_generator& rmg = default_read_monitor_generator()) const;

    flat_mutation_reader make_reader(
            schema_ptr,
            reader_permit,
            const dht::partition_range&,
            const query::partition_slice&,
            const io_priority_class&,
            tracing::trace_state_ptr,
            streamed_mutation::forwarding,
            mutation_reader::forwarding,
            read_monitor_generator& rmg = default_read_monitor_generator()) const;
};

// This structure holds non-owning reference to multiple sstable sets and allow
// their operations to be combined.
template <std::size_t N>
class compound_sstable_set {
    using sstable_set_ptr = lw_shared_ptr<sstable_set>;
    std::array<sstable_set_ptr*, N> _sets;
public:
    compound_sstable_set(std::array<sstable_set_ptr*, N> sets) : _sets(std::move(sets)) {}

    compound_sstable_set(const compound_sstable_set&) = delete;
    compound_sstable_set& operator=(const compound_sstable_set&) = delete;

    std::vector<lw_shared_ptr<sstable_set>> sets() const {
        return boost::copy_range<std::vector<sstable_set_ptr>>(_sets
                | boost::adaptors::transformed([] (sstable_set_ptr* set) -> sstable_set_ptr {
            return *set;
        }));
    }

    // Allows iteration over all sstables from all sets
    void for_each_sstable(std::function<void(const sstables::shared_sstable&)> func) const {
        for (sstable_set_ptr* set : _sets) {
            for (const shared_sstable& sstable : *(*set)->all()) {
                func(sstable);
            }
        }
    }

    // Returns the amount of sstables in all sets
    uint64_t sstables_size() const {
        return boost::accumulate(_sets | boost::adaptors::transformed([] (sstable_set_ptr* set) -> uint64_t {
            return (*set)->all()->size();
        }), uint64_t(0));
    }

    // Returns sstables from all sets which potentially contain data in the provided range
    std::vector<shared_sstable> select(const dht::partition_range& range) const {
        std::vector<shared_sstable> ret;
        for (auto& set : _sets) {
            auto ssts = (*set)->select(range);
            if (ret.empty()) {
                ret = std::move(ssts);
            } else {
                ret.reserve(ret.size() + ssts.size());
                std::move(ssts.begin(), ssts.end(), std::back_inserter(ret));
            }
        }
        return ret;
    }
};

/// Read a range from the passed-in sstables.
///
/// The reader is restricted, that is it will wait for admission on the semaphore
/// belonging to the passed-in permit, before starting to read.
flat_mutation_reader make_restricted_range_sstable_reader(
    lw_shared_ptr<sstable_set> sstables,
    schema_ptr,
    reader_permit,
    const dht::partition_range&,
    const query::partition_slice&,
    const io_priority_class&,
    tracing::trace_state_ptr,
    streamed_mutation::forwarding,
    mutation_reader::forwarding,
    read_monitor_generator& rmg = default_read_monitor_generator());

sstable_set make_partitioned_sstable_set(schema_ptr schema, lw_shared_ptr<sstable_list> all, bool use_level_metadata = true);

std::ostream& operator<<(std::ostream& os, const sstables::sstable_run& run);

using enable_backlog_tracker = bool_class<class enable_backlog_tracker_tag>;

using enable_offstrategy = bool_class<class enable_offstrategy_tag>;

struct sstables_mutation_source {
    mutation_source_opt single_key;
    mutation_source_opt range_scanning;
};

}
