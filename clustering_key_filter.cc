/*
 * Copyright (C) 2016 ScyllaDB
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

#include "clustering_key_filter.hh"
#include "keys.hh"
#include "query-request.hh"

namespace query {

static thread_local clustering_row_ranges full_range = {{}};

const clustering_row_ranges&
clustering_key_filtering_context::get_ranges(const partition_key& key) const {
    return _factory ? _factory->get_ranges(key) : full_range;
}

clustering_row_ranges
clustering_key_filtering_context::get_all_ranges() const {
    return _factory ? _factory->get_all_ranges() : full_range;
}

clustering_key_filtering_context clustering_key_filtering_context::create_no_filtering() {
    return clustering_key_filtering_context{};
}

const clustering_key_filtering_context no_clustering_key_filtering =
    clustering_key_filtering_context::create_no_filtering();

class stateless_clustering_key_filter_factory : public clustering_key_filter_factory {
    clustering_key_filter _filter;
    clustering_row_ranges _ranges;
public:
    stateless_clustering_key_filter_factory(clustering_row_ranges&& ranges,
                                    clustering_key_filter&& filter)
        : _filter(std::move(filter)), _ranges(std::move(ranges)) {}

    virtual clustering_key_filter get_filter(const partition_key& key) override {
        return _filter;
    }

    virtual clustering_key_filter get_filter_for_sorted(const partition_key& key) override {
        return _filter;
    }

    virtual const clustering_row_ranges& get_ranges(const partition_key& key) override {
        return _ranges;
    }

    virtual bool want_static_columns(const partition_key& key) override {
        return true;
    }

    virtual clustering_row_ranges get_all_ranges() override {
        return _ranges;
    }
};

class partition_slice_clustering_key_filter_factory : public clustering_key_filter_factory {
    schema_ptr _schema;
    const partition_slice& _slice;
    clustering_key_prefix::prefix_equal_tri_compare _cmp;
    clustering_row_ranges _ck_ranges;
public:
    partition_slice_clustering_key_filter_factory(schema_ptr s, const partition_slice& slice)
        : _schema(std::move(s)), _slice(slice), _cmp(*_schema) {}

    virtual clustering_key_filter get_filter(const partition_key& key) override {
        const clustering_row_ranges& ranges = _slice.row_ranges(*_schema, key);
        return [this, &ranges] (const clustering_key& key) {
            return std::any_of(std::begin(ranges), std::end(ranges),
                [this, &key] (const clustering_range& r) { return r.contains(key, _cmp); });
        };
    }

    virtual clustering_key_filter get_filter_for_sorted(const partition_key& key) override {
        const clustering_row_ranges& ranges = _slice.row_ranges(*_schema, key);
        return [this, &ranges] (const clustering_key& key) {
            return std::any_of(std::begin(ranges), std::end(ranges),
                [this, &key] (const clustering_range& r) { return r.contains(key, _cmp); });
        };
    }

    virtual const clustering_row_ranges& get_ranges(const partition_key& key) override {
        if (_slice.options.contains(query::partition_slice::option::reversed)) {
            _ck_ranges = _slice.row_ranges(*_schema, key);
            std::reverse(_ck_ranges.begin(), _ck_ranges.end());
            return _ck_ranges;
        }
        return _slice.row_ranges(*_schema, key);
    }

    virtual bool want_static_columns(const partition_key& key) override {
        return true;
    }

    virtual clustering_row_ranges get_all_ranges() override {
        auto all_ranges = _slice.default_row_ranges();
        const auto& specific_ranges = _slice.get_specific_ranges();
        if (specific_ranges) {
            all_ranges.insert(all_ranges.end(), specific_ranges->ranges().begin(), specific_ranges->ranges().end());
        }
        return all_ranges;
    }
};

static const shared_ptr<clustering_key_filter_factory>
create_partition_slice_filter(schema_ptr s, const partition_slice& slice) {
    return ::make_shared<partition_slice_clustering_key_filter_factory>(std::move(s), slice);
}

const clustering_key_filtering_context
clustering_key_filtering_context::create(schema_ptr schema, const partition_slice& slice) {
    static thread_local clustering_key_filtering_context accept_all = clustering_key_filtering_context(
        ::make_shared<stateless_clustering_key_filter_factory>(clustering_row_ranges{{}},
                                                       [](const clustering_key&) { return true; }));
    static thread_local clustering_key_filtering_context reject_all = clustering_key_filtering_context(
        ::make_shared<stateless_clustering_key_filter_factory>(clustering_row_ranges{},
                                                       [](const clustering_key&) { return false; }));

    if (slice.get_specific_ranges()) {
        return clustering_key_filtering_context(create_partition_slice_filter(schema, slice));
    }

    const clustering_row_ranges& ranges = slice.default_row_ranges();

    if (ranges.empty()) {
        return reject_all;
    }

    if (ranges.size() == 1 && ranges[0].is_full()) {
        return accept_all;
    }
    return clustering_key_filtering_context(create_partition_slice_filter(schema, slice));
}

}
