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

#include "sstables.hh"
#include "query-request.hh" // for partition_range; FIXME: move it out of there
#include <seastar/core/shared_ptr.hh>
#include <vector>

namespace sstables {

class sstable_set {
public:
    virtual ~sstable_set();
    virtual shared_ptr<sstable_set> clone() const = 0;
    virtual std::vector<shared_sstable> select(const query::partition_range& range) const = 0;
    virtual std::vector<shared_sstable> all() const = 0;
    virtual void insert(shared_sstable sst) = 0;
    virtual void erase(shared_sstable sst) = 0;
};

}
