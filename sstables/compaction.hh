/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 *
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

#include "sstables.hh"
#include <functional>

namespace sstables {

    struct compaction_descriptor {
        // List of sstables to be compacted.
        std::vector<sstables::shared_sstable> sstables;
        // Level of sstable(s) created by compaction procedure.
        int level = 0;
        // Threshold size for sstable(s) to be created.
        uint64_t max_sstable_bytes = std::numeric_limits<uint64_t>::max();

        compaction_descriptor() = default;

        compaction_descriptor(std::vector<sstables::shared_sstable> sstables, int level, long max_sstable_bytes)
            : sstables(std::move(sstables))
            , level(level)
            , max_sstable_bytes(max_sstable_bytes) {}

        compaction_descriptor(std::vector<sstables::shared_sstable> sstables)
            : sstables(std::move(sstables)) {}
    };

    future<> compact_sstables(std::vector<shared_sstable> sstables,
            column_family& cf, std::function<shared_sstable()> creator);

    // Return the most interesting bucket applying the size-tiered strategy.
    // NOTE: currently used for purposes of testing. May also be used by leveled compaction strategy.
    std::vector<sstables::shared_sstable>
    size_tiered_most_interesting_bucket(lw_shared_ptr<sstable_list> candidates);
}
