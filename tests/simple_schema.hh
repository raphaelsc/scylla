/*
 * Copyright (C) 2017 ScyllaDB
 *
 * Modified by ScyllaDB
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

#include "make_random_string.hh"
#include "schema.hh"
#include "keys.hh"

// Helper for working with the following table:
//
//   CREATE TABLE ks.cf (pk utf8, ck utf8, v utf8, s1 utf8 static, PRIMARY KEY (pk, ck));
//
class simple_schema {
    schema_ptr _s;
public:
    simple_schema()
        : _s(schema_builder("ks", "cf")
            .with_column("pk", utf8_type, column_kind::partition_key)
            .with_column("ck", utf8_type, column_kind::clustering_key)
            .with_column("s1", utf8_type, column_kind::static_column)
            .with_column("v", utf8_type)
            .build())
    { }

    clustering_key make_ckey(sstring ck) {
        return clustering_key::from_single_value(*_s, data_value(ck).serialize());
    }

    // Make a clustering_key which is n-th in some arbitrary sequence of keys
    clustering_key make_ckey(uint32_t n) {
        return make_ckey(sprint("ck%010d", n));
    }

    partition_key make_pkey(sstring pk) {
        return partition_key::from_single_value(*_s, data_value(pk).serialize());
    }

    void add_row(mutation& m, const clustering_key& key, sstring v) {
        m.set_clustered_cell(key, to_bytes("v"), data_value(v), new_timestamp());
    }

    void add_static_row(mutation& m, sstring s1) {
        m.set_static_cell(to_bytes("s1"), data_value(s1), new_timestamp());
    }

    range_tombstone delete_range(mutation& m, const query::clustering_range& range) {
        auto bv_range = bound_view::from_range(range);
        range_tombstone rt(bv_range.first, bv_range.second, tombstone(new_timestamp(), gc_clock::now()));
        m.partition().apply_delete(*_s, rt);
        return rt;
    }

    mutation new_mutation(sstring pk) {
        return mutation(make_pkey(pk), _s);
    }

    schema_ptr schema() {
        return _s;
    }
};
