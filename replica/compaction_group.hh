/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/core/condition-variable.hh>

#include "database_fwd.hh"
#include "compaction/compaction_descriptor.hh"
#include "compaction/compaction_backlog_manager.hh"
#include "compaction/compaction_strategy_state.hh"
#include "sstables/sstable_set.hh"
#include "compaction/compaction_fwd.hh"

#pragma once

namespace replica {

using enable_backlog_tracker = bool_class<class enable_backlog_tracker_tag>;

// Compaction group is a set of SSTables which are eligible to be compacted together.
// By this definition, we can say:
//      - A group contains SSTables that are owned by the same shard.
//      - Also, a group will be owned by a single table. Different tables own different groups.
//      - Each group can be thought of an isolated LSM tree, where Memtable(s) and SSTable(s) are
//          isolated from other groups.
// Usually, a table T in shard S will own a single compaction group. With compaction_group, a
// table T will be able to own as many groups as it wishes.
class compaction_group {
    table& _t;
    class table_state;
    std::unique_ptr<table_state> _table_state;
    const size_t _group_id;
    // Tokens included in this compaction_groups
    dht::token_range _token_range;
    compaction::compaction_strategy_state _compaction_strategy_state;
    // Holds list of memtables for this group
    lw_shared_ptr<memtable_list> _memtables;
    // SSTable set which contains all non-maintenance sstables
    sstables::sstable_set _main_sstables;
    // Holds SSTables created by maintenance operations, which need reshaping before integration into the main set
    sstables::sstable_set _maintenance_sstables;
    // sstables that have been compacted (so don't look up in query) but
    // have not been deleted yet, so must not GC any tombstones in other sstables
    // that may delete data in these sstables:
    std::vector<sstables::shared_sstable> _sstables_compacted_but_not_deleted;
    seastar::condition_variable _staging_done_condition;
private:
    static uint64_t calculate_disk_space_used_for(const sstables::sstable_set& set);

    future<> delete_sstables_atomically(std::vector<sstables::shared_sstable> sstables_to_remove);
public:
    compaction_group(table& t, size_t gid, dht::token_range token_range);

    size_t group_id() const noexcept {
        return _group_id;
    }

    // Will stop ongoing compaction on behalf of this group, etc.
    future<> stop() noexcept;

    // Clear sstable sets
    void clear_sstables();

    // Clear memtable(s) content
    future<> clear_memtables();

    future<> flush();
    bool can_flush() const;

    const dht::token_range& token_range() const noexcept {
        return _token_range;
    }

    void set_compaction_strategy_state(compaction::compaction_strategy_state compaction_strategy_state) noexcept;

    lw_shared_ptr<memtable_list>& memtables() noexcept;
    size_t memtable_count() const noexcept;
    // Returns minimum timestamp from memtable list
    api::timestamp_type min_memtable_timestamp() const;

    // Update main sstable set based on info in completion descriptor, where input sstables
    // will be replaced by output ones, row cache ranges are possibly invalidated and
    // statistics are updated.
    future<> update_main_sstable_list_on_compaction_completion(sstables::compaction_completion_desc desc);

    // This will update sstable lists on behalf of off-strategy compaction, where
    // input files will be removed from the maintenance set and output files will
    // be inserted into the main set.
    future<> update_sstable_lists_on_off_strategy_completion(sstables::compaction_completion_desc desc);

    const sstables::sstable_set& main_sstables() const noexcept;
    void set_main_sstables(sstables::sstable_set&& new_main_sstables);

    const sstables::sstable_set& maintenance_sstables() const noexcept;
    void set_maintenance_sstables(sstables::sstable_set&& new_maintenance_sstables);

    // Makes a compound set, which includes main and maintenance sets
    sstables::sstable_set make_compound_sstable_set();

    const std::vector<sstables::shared_sstable>& compacted_undeleted_sstables() const noexcept;
    // Triggers regular compaction.
    void trigger_compaction();

    compaction_backlog_tracker& get_backlog_tracker();

    size_t live_sstable_count() const noexcept;
    uint64_t live_disk_space_used() const noexcept;
    uint64_t total_disk_space_used() const noexcept;

    compaction::table_state& as_table_state() const noexcept;

    seastar::condition_variable& get_staging_done_condition() noexcept {
        return _staging_done_condition;
    }
};

using compaction_group_vector = utils::chunked_vector<std::unique_ptr<compaction_group>>;

class compaction_group_manager {
public:
    virtual ~compaction_group_manager() {}
    virtual compaction_group_vector make_compaction_groups() const = 0;
    virtual size_t compaction_group_of(dht::token) const = 0;
    virtual size_t log2_compaction_groups() const = 0;
};

}
