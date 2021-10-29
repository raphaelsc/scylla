/*
 * Copyright (C) 2021-present ScyllaDB
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

#include "schema_fwd.hh"
#include "sstables/sstable_set.hh"

class table_state_impl {
public:
    virtual ~table_state_impl() {}
    virtual schema_ptr schema() const = 0;
    // min threshold as defined by table.
    virtual int min_compaction_threshold() const = 0;
    virtual bool enforce_min_compaction_threshold() const = 0;
    virtual bool has_ongoing_compaction() const = 0;
    virtual const sstables::sstable_set& sstable_set() const = 0;
    virtual std::unordered_set<sstables::shared_sstable> fully_expired_sstables(const std::vector<sstables::shared_sstable>& sstables) const = 0;
};

class table_state {
    ::shared_ptr<table_state_impl> _impl;
public:
    explicit table_state(::shared_ptr<table_state_impl> impl) : _impl(std::move(impl)) {}

    schema_ptr schema() const {
        return _impl->schema();
    }
    int min_compaction_threshold() const {
        return _impl->min_compaction_threshold();
    }
    bool enforce_min_compaction_threshold() const {
        return _impl->enforce_min_compaction_threshold();
    }
    bool has_ongoing_compaction() const {
        return _impl->has_ongoing_compaction();
    }
    const sstables::sstable_set& sstable_set() const {
        return _impl->sstable_set();
    }
    std::unordered_set<sstables::shared_sstable> fully_expired_sstables(const std::vector<sstables::shared_sstable>& sstables) const {
        return _impl->fully_expired_sstables(sstables);
    }
};
