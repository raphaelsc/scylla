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

#include <set>
#include "partition_slice_builder.hh"
#include "schema_builder.hh"
#include "mutation_reader_assertions.hh"
#include "mutation_assertions.hh"
#include "mutation_source_test.hh"
#include "counters.hh"
#include "simple_schema.hh"

// partitions must be sorted by decorated key
static void require_no_token_duplicates(const std::vector<mutation>& partitions) {
    std::experimental::optional<dht::token> last_token;
    for (auto&& p : partitions) {
        const dht::decorated_key& key = p.decorated_key();
        if (last_token && key.token() == *last_token) {
            BOOST_FAIL("token duplicate detected");
        }
        last_token = key.token();
    }
}

static api::timestamp_type new_timestamp() {
    static thread_local api::timestamp_type ts = api::min_timestamp;
    return ts++;
}

static void test_streamed_mutation_forwarding_is_consistent_with_slicing(populate_fn populate) {
    BOOST_TEST_MESSAGE(__PRETTY_FUNCTION__);

    // Generates few random mutations and row slices and verifies that using
    // fast_forward_to() over the slices gives the same mutations as using those
    // slices in partition_slice without forwarding.

    random_mutation_generator gen(random_mutation_generator::generate_counters::no);

    for (int i = 0; i < 10; ++i) {
        mutation m = gen();

        std::vector<query::clustering_range> ranges = gen.make_random_ranges(10);
        auto prange = dht::partition_range::make_singular(m.decorated_key());
        query::partition_slice full_slice = partition_slice_builder(*m.schema()).build();
        query::partition_slice slice_with_ranges = partition_slice_builder(*m.schema())
            .with_ranges(ranges)
            .build();

        BOOST_TEST_MESSAGE(sprint("ranges: %s", ranges));

        mutation_source ms = populate(m.schema(), {m});

        streamed_mutation sliced_sm = [&] {
            mutation_reader rd = ms(m.schema(), prange, slice_with_ranges);
            streamed_mutation_opt smo = rd().get0();
            BOOST_REQUIRE(bool(smo));
            return std::move(*smo);
        }();

        streamed_mutation fwd_sm = [&] {
            mutation_reader rd = ms(m.schema(), prange, full_slice, default_priority_class(), nullptr, streamed_mutation::forwarding::yes);
            streamed_mutation_opt smo = rd().get0();
            BOOST_REQUIRE(bool(smo));
            return std::move(*smo);
        }();

        mutation fwd_m = mutation_from_streamed_mutation(fwd_sm).get0();
        for (auto&& range : ranges) {
            BOOST_TEST_MESSAGE(sprint("fwd %s", range));
            fwd_sm.fast_forward_to(position_range(range));
            fwd_m += mutation_from_streamed_mutation(fwd_sm).get0();
        }

        mutation sliced_m = mutation_from_streamed_mutation(sliced_sm).get0();
        assert_that(sliced_m).is_equal_to(fwd_m);
    }
}

static void test_streamed_mutation_forwarding_guarantees(populate_fn populate) {
    BOOST_TEST_MESSAGE(__PRETTY_FUNCTION__);

    simple_schema table;
    schema_ptr s = table.schema();

    // mutation will include odd keys
    auto contains_key = [] (int i) {
        return i % 2 == 1;
    };

    const int n_keys = 1001;
    assert(!contains_key(n_keys - 1)); // so that we can form a range with position greater than all keys

    mutation m(table.make_pkey("pkey1"), s);
    std::vector<clustering_key> keys;
    for (int i = 0; i < n_keys; ++i) {
        keys.push_back(table.make_ckey(i));
        if (contains_key(i)) {
            table.add_row(m, keys.back(), "value");
        }
    }

    table.add_static_row(m, "static_value");

    mutation_source ms = populate(s, std::vector<mutation>({m}));

    auto new_stream = [&ms, s] () -> streamed_mutation_assertions {
        BOOST_TEST_MESSAGE("Creating new streamed_mutation");
        mutation_reader rd = ms(s,
            query::full_partition_range,
            query::full_slice,
            default_priority_class(),
            nullptr,
            streamed_mutation::forwarding::yes);

        streamed_mutation_opt smo = rd().get0();
        BOOST_REQUIRE(bool(smo));
        return assert_that_stream(std::move(*smo));
    };

    auto verify_range = [&] (streamed_mutation_assertions& sm, int start, int end) {
        sm.fwd_to(keys[start], keys[end]);

        for (; start < end; ++start) {
            if (!contains_key(start)) {
                BOOST_TEST_MESSAGE(sprint("skip %d", start));
                continue;
            }
            sm.produces_row_with_key(keys[start]);
        }
        sm.produces_end_of_stream();
    };

    // Test cases start here

    {
        auto sm = new_stream();
        sm.produces_static_row();
        sm.produces_end_of_stream();
    }

    {
        auto sm = new_stream();
        verify_range(sm, 0, 1);
        verify_range(sm, 1, 2);
        verify_range(sm, 2, 4);
        verify_range(sm, 7, 7);
        verify_range(sm, 7, 9);
        verify_range(sm, 11, 15);
        verify_range(sm, 21, 32);
        verify_range(sm, 132, 200);
        verify_range(sm, 300, n_keys - 1);
    }

    // Skip before EOS
    {
        auto sm = new_stream();
        sm.fwd_to(keys[0], keys[4]);
        sm.produces_row_with_key(keys[1]);
        sm.fwd_to(keys[5], keys[8]);
        sm.produces_row_with_key(keys[5]);
        sm.produces_row_with_key(keys[7]);
        sm.produces_end_of_stream();
        sm.fwd_to(keys[9], keys[12]);
        sm.fwd_to(keys[12], keys[13]);
        sm.fwd_to(keys[13], keys[13]);
        sm.produces_end_of_stream();
        sm.fwd_to(keys[13], keys[16]);
        sm.produces_row_with_key(keys[13]);
        sm.produces_row_with_key(keys[15]);
        sm.produces_end_of_stream();
    }

    {
        auto sm = new_stream();
        verify_range(sm, n_keys - 2, n_keys - 1);
    }

    {
        auto sm = new_stream();
        verify_range(sm, 0, n_keys - 1);
    }

    // Few random ranges
    std::default_random_engine rnd;
    std::uniform_int_distribution<int> key_dist{0, n_keys - 1};
    for (int i = 0; i < 10; ++i) {
        std::vector<int> indices;
        const int n_ranges = 7;
        for (int j = 0; j < n_ranges * 2; ++j) {
            indices.push_back(key_dist(rnd));
        }
        std::sort(indices.begin(), indices.end());

        auto sm = new_stream();
        for (int j = 0; j < n_ranges; ++j) {
            verify_range(sm, indices[j*2], indices[j*2 + 1]);
        }
    }
}

static void test_streamed_mutation_slicing_returns_only_relevant_tombstones(populate_fn populate) {
    BOOST_TEST_MESSAGE(__PRETTY_FUNCTION__);

    simple_schema table;
    schema_ptr s = table.schema();

    mutation m(table.make_pkey("pkey1"), s);

    std::vector<clustering_key> keys;
    for (int i = 0; i < 20; ++i) {
        keys.push_back(table.make_ckey(i));
    }

    auto rt1 = table.delete_range(m, query::clustering_range::make(
        query::clustering_range::bound(keys[0], true),
        query::clustering_range::bound(keys[1], true)
    ));

    table.add_row(m, keys[2], "value");

    auto rt2 = table.delete_range(m, query::clustering_range::make(
        query::clustering_range::bound(keys[3], true),
        query::clustering_range::bound(keys[4], true)
    ));

    table.add_row(m, keys[5], "value");

    auto rt3 = table.delete_range(m, query::clustering_range::make(
        query::clustering_range::bound(keys[6], true),
        query::clustering_range::bound(keys[7], true)
    ));

    table.add_row(m, keys[8], "value");

    auto rt4 = table.delete_range(m, query::clustering_range::make(
        query::clustering_range::bound(keys[9], true),
        query::clustering_range::bound(keys[10], true)
    ));

    auto rt5 = table.delete_range(m, query::clustering_range::make(
        query::clustering_range::bound(keys[11], true),
        query::clustering_range::bound(keys[12], true)
    ));

    table.add_row(m, keys[10], "value");

    auto slice = partition_slice_builder(*s)
        .with_range(query::clustering_range::make(
            query::clustering_range::bound(keys[2], true),
            query::clustering_range::bound(keys[2], true)
        ))
        .with_range(query::clustering_range::make(
            query::clustering_range::bound(keys[7], true),
            query::clustering_range::bound(keys[9], true)
        ))
        .build();

    mutation_source ms = populate(s, std::vector<mutation>({m}));
    mutation_reader rd = ms(s, query::full_partition_range, slice);

    streamed_mutation_opt smo = rd().get0();
    BOOST_REQUIRE(bool(smo));
    auto sm = assert_that_stream(std::move(*smo));

    sm.produces_row_with_key(keys[2]);
    sm.produces_range_tombstone(rt3);
    sm.produces_row_with_key(keys[8]);
    sm.produces_range_tombstone(rt4);
    sm.produces_end_of_stream();
}

static void test_streamed_mutation_forwarding_across_range_tombstones(populate_fn populate) {
    BOOST_TEST_MESSAGE(__PRETTY_FUNCTION__);

    simple_schema table;
    schema_ptr s = table.schema();

    mutation m(table.make_pkey("pkey1"), s);

    std::vector<clustering_key> keys;
    for (int i = 0; i < 20; ++i) {
        keys.push_back(table.make_ckey(i));
    }

    auto rt1 = table.delete_range(m, query::clustering_range::make(
        query::clustering_range::bound(keys[0], true),
        query::clustering_range::bound(keys[1], false)
    ));

    table.add_row(m, keys[2], "value");

    auto rt2 = table.delete_range(m, query::clustering_range::make(
        query::clustering_range::bound(keys[3], true),
        query::clustering_range::bound(keys[6], true)
    ));

    table.add_row(m, keys[4], "value");

    auto rt3 = table.delete_range(m, query::clustering_range::make(
        query::clustering_range::bound(keys[7], true),
        query::clustering_range::bound(keys[8], true)
    ));

    auto rt4 = table.delete_range(m, query::clustering_range::make(
        query::clustering_range::bound(keys[9], true),
        query::clustering_range::bound(keys[10], true)
    ));

    auto rt5 = table.delete_range(m, query::clustering_range::make(
        query::clustering_range::bound(keys[11], true),
        query::clustering_range::bound(keys[13], true)
    ));

    mutation_source ms = populate(s, std::vector<mutation>({m}));
    mutation_reader rd = ms(s,
        query::full_partition_range,
        query::full_slice,
        default_priority_class(),
        nullptr,
        streamed_mutation::forwarding::yes);

    streamed_mutation_opt smo = rd().get0();
    BOOST_REQUIRE(bool(smo));
    auto sm = assert_that_stream(std::move(*smo));

    sm.fwd_to(position_range(query::clustering_range::make(
        query::clustering_range::bound(keys[1], true),
        query::clustering_range::bound(keys[2], true)
    )));

    sm.produces_row_with_key(keys[2]);

    sm.fwd_to(position_range(query::clustering_range::make(
        query::clustering_range::bound(keys[4], true),
        query::clustering_range::bound(keys[8], false)
    )));

    sm.produces_range_tombstone(rt2);
    sm.produces_row_with_key(keys[4]);
    sm.produces_range_tombstone(rt3);

    sm.fwd_to(position_range(query::clustering_range::make(
        query::clustering_range::bound(keys[10], true),
        query::clustering_range::bound(keys[12], false)
    )));

    sm.produces_range_tombstone(rt4);
    sm.produces_range_tombstone(rt5);
    sm.produces_end_of_stream();

    sm.fwd_to(position_range(query::clustering_range::make(
        query::clustering_range::bound(keys[14], true),
        query::clustering_range::bound(keys[15], false)
    )));

    sm.produces_end_of_stream();

    sm.fwd_to(position_range(query::clustering_range::make(
        query::clustering_range::bound(keys[15], true),
        query::clustering_range::bound(keys[16], false)
    )));

    sm.produces_end_of_stream();
}

static void test_range_queries(populate_fn populate) {
    BOOST_TEST_MESSAGE("Testing range queries");

    auto s = schema_builder("ks", "cf")
        .with_column("key", bytes_type, column_kind::partition_key)
        .with_column("v", bytes_type)
        .build();

    auto make_partition_mutation = [s] (bytes key) -> mutation {
        mutation m(partition_key::from_single_value(*s, key), s);
        m.set_clustered_cell(clustering_key::make_empty(), "v", data_value(bytes("v1")), 1);
        return m;
    };

    int partition_count = 300;

    std::vector<mutation> partitions;
    for (int i = 0; i < partition_count; ++i) {
        partitions.emplace_back(
            make_partition_mutation(to_bytes(sprint("key_%d", i))));
    }

    std::sort(partitions.begin(), partitions.end(), mutation_decorated_key_less_comparator());
    require_no_token_duplicates(partitions);

    dht::decorated_key key_before_all = partitions.front().decorated_key();
    partitions.erase(partitions.begin());

    dht::decorated_key key_after_all = partitions.back().decorated_key();
    partitions.pop_back();

    auto ds = populate(s, partitions);

    auto test_slice = [&] (dht::partition_range r) {
        BOOST_TEST_MESSAGE(sprint("Testing range %s", r));
        assert_that(ds(s, r))
            .produces(slice(partitions, r))
            .produces_end_of_stream();
    };

    auto inclusive_token_range = [&] (size_t start, size_t end) {
        return dht::partition_range::make(
            {dht::ring_position::starting_at(partitions[start].token())},
            {dht::ring_position::ending_at(partitions[end].token())});
    };

    test_slice(dht::partition_range::make(
        {key_before_all, true}, {partitions.front().decorated_key(), true}));

    test_slice(dht::partition_range::make(
        {key_before_all, false}, {partitions.front().decorated_key(), true}));

    test_slice(dht::partition_range::make(
        {key_before_all, false}, {partitions.front().decorated_key(), false}));

    test_slice(dht::partition_range::make(
        {dht::ring_position::starting_at(key_before_all.token())},
        {dht::ring_position::ending_at(partitions.front().token())}));

    test_slice(dht::partition_range::make(
        {dht::ring_position::ending_at(key_before_all.token())},
        {dht::ring_position::ending_at(partitions.front().token())}));

    test_slice(dht::partition_range::make(
        {dht::ring_position::ending_at(key_before_all.token())},
        {dht::ring_position::starting_at(partitions.front().token())}));

    test_slice(dht::partition_range::make(
        {partitions.back().decorated_key(), true}, {key_after_all, true}));

    test_slice(dht::partition_range::make(
        {partitions.back().decorated_key(), true}, {key_after_all, false}));

    test_slice(dht::partition_range::make(
        {partitions.back().decorated_key(), false}, {key_after_all, false}));

    test_slice(dht::partition_range::make(
        {dht::ring_position::starting_at(partitions.back().token())},
        {dht::ring_position::ending_at(key_after_all.token())}));

    test_slice(dht::partition_range::make(
        {dht::ring_position::starting_at(partitions.back().token())},
        {dht::ring_position::starting_at(key_after_all.token())}));

    test_slice(dht::partition_range::make(
        {dht::ring_position::ending_at(partitions.back().token())},
        {dht::ring_position::starting_at(key_after_all.token())}));

    test_slice(dht::partition_range::make(
        {partitions[0].decorated_key(), false},
        {partitions[1].decorated_key(), true}));

    test_slice(dht::partition_range::make(
        {partitions[0].decorated_key(), true},
        {partitions[1].decorated_key(), false}));

    test_slice(dht::partition_range::make(
        {partitions[1].decorated_key(), true},
        {partitions[3].decorated_key(), false}));

    test_slice(dht::partition_range::make(
        {partitions[1].decorated_key(), false},
        {partitions[3].decorated_key(), true}));

    test_slice(dht::partition_range::make_ending_with(
        {partitions[3].decorated_key(), true}));

    test_slice(dht::partition_range::make_starting_with(
        {partitions[partitions.size() - 4].decorated_key(), true}));

    test_slice(inclusive_token_range(0, 0));
    test_slice(inclusive_token_range(1, 1));
    test_slice(inclusive_token_range(2, 4));
    test_slice(inclusive_token_range(127, 128));
    test_slice(inclusive_token_range(128, 128));
    test_slice(inclusive_token_range(128, 129));
    test_slice(inclusive_token_range(127, 129));
    test_slice(inclusive_token_range(partitions.size() - 1, partitions.size() - 1));

    test_slice(inclusive_token_range(0, partitions.size() - 1));
    test_slice(inclusive_token_range(0, partitions.size() - 2));
    test_slice(inclusive_token_range(0, partitions.size() - 3));
    test_slice(inclusive_token_range(0, partitions.size() - 128));

    test_slice(inclusive_token_range(1, partitions.size() - 1));
    test_slice(inclusive_token_range(2, partitions.size() - 1));
    test_slice(inclusive_token_range(3, partitions.size() - 1));
    test_slice(inclusive_token_range(128, partitions.size() - 1));
}

void ensure_monotonic_positions(streamed_mutation& sm) {
    position_in_partition::less_compare less(*sm.schema());
    mutation_fragment_opt prev;
    for (;;) {
        mutation_fragment_opt mfo = sm().get0();
        if (!mfo) {
            break;
        }
        if (prev) {
            if (!less(prev->position(), mfo->position())) {
                BOOST_FAIL(sprint("previous fragment has greater position: prev=%s, current=%s", *prev, *mfo));
            }
        }
        prev = std::move(mfo);
    }
}

void test_streamed_mutation_fragments_have_monotonic_positions(populate_fn populate) {
    BOOST_TEST_MESSAGE(__PRETTY_FUNCTION__);

    for_each_mutation([] (const mutation& m) {
        streamed_mutation sm = streamed_mutation_from_mutation(m);
        ensure_monotonic_positions(sm);
    });
}

void run_mutation_source_tests(populate_fn populate) {
    test_streamed_mutation_fragments_have_monotonic_positions(populate);
    test_streamed_mutation_forwarding_across_range_tombstones(populate);
    test_streamed_mutation_forwarding_guarantees(populate);
    test_streamed_mutation_slicing_returns_only_relevant_tombstones(populate);
    test_streamed_mutation_forwarding_is_consistent_with_slicing(populate);
    test_range_queries(populate);
}

struct mutation_sets {
    std::vector<std::vector<mutation>> equal;
    std::vector<std::vector<mutation>> unequal;
    mutation_sets(){}
};

static tombstone new_tombstone() {
    return { new_timestamp(), gc_clock::now() };
}

static mutation_sets generate_mutation_sets() {
    using mutations = std::vector<mutation>;
    mutation_sets result;

    {
        auto common_schema = schema_builder("ks", "test")
                .with_column("pk_col", bytes_type, column_kind::partition_key)
                .with_column("ck_col_1", bytes_type, column_kind::clustering_key)
                .with_column("ck_col_2", bytes_type, column_kind::clustering_key)
                .with_column("regular_col_1", bytes_type)
                .with_column("regular_col_2", bytes_type)
                .with_column("static_col_1", bytes_type, column_kind::static_column)
                .with_column("static_col_2", bytes_type, column_kind::static_column);

        auto s1 = common_schema
                .with_column("regular_col_1_s1", bytes_type) // will have id in between common columns
                .build();

        auto s2 = common_schema
                .with_column("regular_col_1_s2", bytes_type) // will have id in between common columns
                .build();

        // Differing keys
        result.unequal.emplace_back(mutations{
            mutation(partition_key::from_single_value(*s1, to_bytes("key1")), s1),
            mutation(partition_key::from_single_value(*s2, to_bytes("key2")), s2)
        });

        auto m1 = mutation(partition_key::from_single_value(*s1, to_bytes("key1")), s1);
        auto m2 = mutation(partition_key::from_single_value(*s2, to_bytes("key1")), s2);

        result.equal.emplace_back(mutations{m1, m2});

        clustering_key ck1 = clustering_key::from_deeply_exploded(*s1, {data_value(bytes("ck1_0")), data_value(bytes("ck1_1"))});
        clustering_key ck2 = clustering_key::from_deeply_exploded(*s1, {data_value(bytes("ck2_0")), data_value(bytes("ck2_1"))});
        auto ttl = gc_clock::duration(1);

        {
            auto tomb = new_tombstone();
            m1.partition().apply(tomb);
            result.unequal.emplace_back(mutations{m1, m2});
            m2.partition().apply(tomb);
            result.equal.emplace_back(mutations{m1, m2});
        }

        {
            auto tomb = new_tombstone();
            m1.partition().apply_delete(*s1, ck2, tomb);
            result.unequal.emplace_back(mutations{m1, m2});
            m2.partition().apply_delete(*s1, ck2, tomb);
            result.equal.emplace_back(mutations{m1, m2});
        }

        {
            auto tomb = new_tombstone();
            auto key = clustering_key_prefix::from_deeply_exploded(*s1, {data_value(bytes("ck2_0"))});
            m1.partition().apply_row_tombstone(*s1, key, tomb);
            result.unequal.emplace_back(mutations{m1, m2});
            m2.partition().apply_row_tombstone(*s1, key, tomb);
            result.equal.emplace_back(mutations{m1, m2});
        }

        {
            auto ts = new_timestamp();
            m1.set_clustered_cell(ck1, "regular_col_1", data_value(bytes("regular_col_value")), ts, ttl);
            result.unequal.emplace_back(mutations{m1, m2});
            m2.set_clustered_cell(ck1, "regular_col_1", data_value(bytes("regular_col_value")), ts, ttl);
            result.equal.emplace_back(mutations{m1, m2});
        }

        {
            auto ts = new_timestamp();
            m1.set_clustered_cell(ck1, "regular_col_2", data_value(bytes("regular_col_value")), ts, ttl);
            result.unequal.emplace_back(mutations{m1, m2});
            m2.set_clustered_cell(ck1, "regular_col_2", data_value(bytes("regular_col_value")), ts, ttl);
            result.equal.emplace_back(mutations{m1, m2});
        }

        {
            auto ts = new_timestamp();
            m1.partition().apply_insert(*s1, ck2, ts);
            result.unequal.emplace_back(mutations{m1, m2});
            m2.partition().apply_insert(*s1, ck2, ts);
            result.equal.emplace_back(mutations{m1, m2});
        }

        {
            auto ts = new_timestamp();
            m1.set_clustered_cell(ck2, "regular_col_1", data_value(bytes("ck2_regular_col_1_value")), ts);
            result.unequal.emplace_back(mutations{m1, m2});
            m2.set_clustered_cell(ck2, "regular_col_1", data_value(bytes("ck2_regular_col_1_value")), ts);
            result.equal.emplace_back(mutations{m1, m2});
        }

        {
            auto ts = new_timestamp();
            m1.set_static_cell("static_col_1", data_value(bytes("static_col_value")), ts, ttl);
            result.unequal.emplace_back(mutations{m1, m2});
            m2.set_static_cell("static_col_1", data_value(bytes("static_col_value")), ts, ttl);
            result.equal.emplace_back(mutations{m1, m2});
        }

        {
            auto ts = new_timestamp();
            m1.set_static_cell("static_col_2", data_value(bytes("static_col_value")), ts);
            result.unequal.emplace_back(mutations{m1, m2});
            m2.set_static_cell("static_col_2", data_value(bytes("static_col_value")), ts);
            result.equal.emplace_back(mutations{m1, m2});
        }

        {
            auto ts = new_timestamp();
            m1.set_clustered_cell(ck2, "regular_col_1_s1", data_value(bytes("x")), ts);
            result.unequal.emplace_back(mutations{m1, m2});
            m2.set_clustered_cell(ck2, "regular_col_1_s2", data_value(bytes("x")), ts);
            result.unequal.emplace_back(mutations{m1, m2});
        }
    }

    static constexpr auto rmg_iterations = 10;

    {
        random_mutation_generator gen(random_mutation_generator::generate_counters::no);
        for (int i = 0; i < rmg_iterations; ++i) {
            auto m = gen();
            result.unequal.emplace_back(mutations{m, gen()}); // collision unlikely
            result.equal.emplace_back(mutations{m, m});
        }
    }

    {
        random_mutation_generator gen(random_mutation_generator::generate_counters::yes);
        for (int i = 0; i < rmg_iterations; ++i) {
            auto m = gen();
            result.unequal.emplace_back(mutations{m, gen()}); // collision unlikely
            result.equal.emplace_back(mutations{m, m});
        }
    }

    return result;
}

static const mutation_sets& get_mutation_sets() {
    static thread_local const auto ms = generate_mutation_sets();
    return ms;
}

void for_each_mutation_pair(std::function<void(const mutation&, const mutation&, are_equal)> callback) {
    auto&& ms = get_mutation_sets();
    for (auto&& mutations : ms.equal) {
        auto i = mutations.begin();
        assert(i != mutations.end());
        const mutation& first = *i++;
        while (i != mutations.end()) {
            callback(first, *i, are_equal::yes);
            ++i;
        }
    }
    for (auto&& mutations : ms.unequal) {
        auto i = mutations.begin();
        assert(i != mutations.end());
        const mutation& first = *i++;
        while (i != mutations.end()) {
            callback(first, *i, are_equal::no);
            ++i;
        }
    }
}

void for_each_mutation(std::function<void(const mutation&)> callback) {
    auto&& ms = get_mutation_sets();
    for (auto&& mutations : ms.equal) {
        for (auto&& m : mutations) {
            callback(m);
        }
    }
    for (auto&& mutations : ms.unequal) {
        for (auto&& m : mutations) {
            callback(m);
        }
    }
}

bytes make_blob(size_t blob_size) {
    static thread_local std::independent_bits_engine<std::default_random_engine, 8, uint8_t> random_bytes;
    bytes big_blob(bytes::initialized_later(), blob_size);
    for (auto&& b : big_blob) {
        b = random_bytes();
    }
    return big_blob;
};

class random_mutation_generator::impl {
    friend class random_mutation_generator;
    generate_counters _generate_counters;
    const size_t _external_blob_size = 128; // Should be enough to force use of external bytes storage
    const size_t n_blobs = 1024;
    const column_id column_count = row::max_vector_size * 2;
    std::mt19937 _gen;
    schema_ptr _schema;
    std::vector<bytes> _blobs;
    std::uniform_int_distribution<size_t> _ck_index_dist{0, n_blobs - 1};
    std::uniform_int_distribution<int> _bool_dist{0, 1};

    static gc_clock::time_point expiry_dist(auto& gen) {
        static thread_local std::uniform_int_distribution<int> dist(0, 2);
        return gc_clock::time_point() + std::chrono::seconds(dist(gen));
    }

    schema_ptr do_make_schema(data_type type) {
        auto builder = schema_builder("ks", "cf")
                .with_column("pk", bytes_type, column_kind::partition_key)
                .with_column("ck1", bytes_type, column_kind::clustering_key)
                .with_column("ck2", bytes_type, column_kind::clustering_key);

        // Create enough columns so that row can overflow its vector storage
        for (column_id i = 0; i < column_count; ++i) {
            {
                auto column_name = sprint("v%d", i);
                builder.with_column(to_bytes(column_name), type, column_kind::regular_column);
            }
            {
                auto column_name = sprint("s%d", i);
                builder.with_column(to_bytes(column_name), type, column_kind::static_column);
            }
        }

        return builder.build();
    }

    schema_ptr make_schema() {
        return _generate_counters ? do_make_schema(counter_type)
                                  : do_make_schema(bytes_type);
    }
public:
    explicit impl(generate_counters counters) : _generate_counters(counters) {
        _schema = make_schema();

        for (size_t i = 0; i < n_blobs; ++i) {
            bytes b(_external_blob_size, int8_t(0));
            std::copy_n(reinterpret_cast<int8_t*>(&i), sizeof(i), b.begin());
            _blobs.emplace_back(std::move(b));
        }

        std::random_device rd;
        // In case of errors, replace the seed with a fixed value to get a deterministic run.
        auto seed = rd();
        BOOST_TEST_MESSAGE(sprint("Random seed: %s", seed));
        _gen = std::mt19937(seed);
    }

    bytes random_blob() {
        return _blobs[std::min(_blobs.size() - 1, std::max<size_t>(0, _ck_index_dist(_gen)))];
    }

    clustering_key make_random_key() {
        return clustering_key::from_exploded(*_schema, { random_blob(), random_blob() });
    }

    clustering_key_prefix make_random_prefix() {
        std::vector<bytes> components = { random_blob() };
        if (_bool_dist(_gen)) {
            components.push_back(random_blob());
        }
        return clustering_key_prefix::from_exploded(*_schema, std::move(components));
    }

    std::vector<query::clustering_range> make_random_ranges(unsigned n_ranges) {
        std::vector<query::clustering_range> ranges;

        if (n_ranges == 0) {
            return ranges;
        }

        auto keys = std::set<clustering_key, clustering_key::less_compare>{clustering_key::less_compare(*_schema)};
        while (keys.size() < n_ranges * 2) {
            keys.insert(make_random_key());
        }

        auto i = keys.begin();

        bool open_start = _bool_dist(_gen);
        bool open_end = _bool_dist(_gen);

        if (open_start && open_end && n_ranges == 1) {
            ranges.push_back(query::clustering_range::make_open_ended_both_sides());
            return ranges;
        }

        if (open_start) {
            ranges.push_back(query::clustering_range(
                { }, { query::clustering_range::bound(*i++, _bool_dist(_gen)) }
            ));
        }

        n_ranges -= unsigned(open_start);
        n_ranges -= unsigned(open_end);

        while (n_ranges--) {
            auto start_key = *i++;
            auto end_key = *i++;
            ranges.push_back(query::clustering_range(
                { query::clustering_range::bound(start_key, _bool_dist(_gen)) },
                { query::clustering_range::bound(end_key, _bool_dist(_gen)) }
            ));
        }

        if (open_end) {
            ranges.push_back(query::clustering_range(
                { query::clustering_range::bound(*i++, _bool_dist(_gen)) }, { }
            ));
        }

        return ranges;
    }

    mutation operator()() {
        std::uniform_int_distribution<column_id> column_count_dist(1, column_count);
        std::uniform_int_distribution<column_id> column_id_dist(0, column_count - 1);
        std::uniform_int_distribution<size_t> value_blob_index_dist(0, 2);

        std::uniform_int_distribution<api::timestamp_type> timestamp_dist(api::min_timestamp, api::min_timestamp + 2); // 3 values

        auto pkey = partition_key::from_single_value(*_schema, _blobs[0]);
        mutation m(pkey, _schema);

        std::map<counter_id, std::set<int64_t>> counter_used_clock_values;
        std::vector<counter_id> counter_ids;
        std::generate_n(std::back_inserter(counter_ids), 8, counter_id::generate_random);

        auto random_counter_cell = [&] {
            std::uniform_int_distribution<size_t> shard_count_dist(1, counter_ids.size());
            std::uniform_int_distribution<int64_t> value_dist(-100, 100);
            std::uniform_int_distribution<int64_t> clock_dist(0, 20000);

            auto shard_count = shard_count_dist(_gen);
            std::set<counter_id> shards;
            for (auto i = 0u; i < shard_count; i++) {
                shards.emplace(counter_ids[shard_count_dist(_gen) - 1]);
            }

            counter_cell_builder ccb;
            for (auto&& id : shards) {
                // Make sure we don't get shards with the same id and clock
                // but different value.
                int64_t clock = clock_dist(_gen);
                while (counter_used_clock_values[id].count(clock)) {
                    clock = clock_dist(_gen);
                }
                counter_used_clock_values[id].emplace(clock);
                ccb.add_shard(counter_shard(id, value_dist(_gen), clock));
            }
            return ccb.build(timestamp_dist(_gen));
        };

        auto set_random_cells = [&] (row& r, column_kind kind) {
            auto columns_to_set = column_count_dist(_gen);
            for (column_id i = 0; i < columns_to_set; ++i) {
                auto cid = column_id_dist(_gen);
                auto get_live_cell = [&] {
                    if (_generate_counters) {
                        return random_counter_cell();
                    } else {
                        return atomic_cell::make_live(timestamp_dist(_gen), _blobs[value_blob_index_dist(_gen)]);
                    }
                };
                // FIXME: generate expiring cells
                auto cell = _bool_dist(_gen)
                            ? get_live_cell()
                            : atomic_cell::make_dead(timestamp_dist(_gen), expiry_dist(_gen));
                r.apply(_schema->column_at(kind, cid), std::move(cell));
            }
        };

        auto random_tombstone = [&] {
            return tombstone(timestamp_dist(_gen), expiry_dist(_gen));
        };

        auto random_row_marker = [&] {
            static thread_local std::uniform_int_distribution<int> dist(0, 3);
            switch (dist(_gen)) {
                case 0: return row_marker();
                case 1: return row_marker(random_tombstone());
                case 2: return row_marker(timestamp_dist(_gen));
                case 3: return row_marker(timestamp_dist(_gen), std::chrono::seconds(1), expiry_dist(_gen));
                default: assert(0);
            }
        };

        if (_bool_dist(_gen)) {
            m.partition().apply(random_tombstone());
        }

        set_random_cells(m.partition().static_row(), column_kind::static_column);

        auto row_count_dist = [&] (auto& gen) {
            static thread_local std::normal_distribution<> dist(32, 1.5);
            return static_cast<size_t>(std::min(100.0, std::max(0.0, dist(gen))));
        };

        size_t row_count = row_count_dist(_gen);
        for (size_t i = 0; i < row_count; ++i) {
            auto ckey = make_random_key();
            deletable_row& row = m.partition().clustered_row(*_schema, ckey);
            set_random_cells(row.cells(), column_kind::regular_column);
            row.marker() = random_row_marker();
        }

        size_t range_tombstone_count = row_count_dist(_gen);
        for (size_t i = 0; i < range_tombstone_count; ++i) {
            auto start = make_random_prefix();
            auto end = make_random_prefix();
            clustering_key_prefix::less_compare less(*_schema);
            if (less(end, start)) {
                std::swap(start, end);
            }
            m.partition().apply_row_tombstone(*_schema,
                    range_tombstone(std::move(start), std::move(end), random_tombstone()));
        }
        return m;
    }
};

random_mutation_generator::~random_mutation_generator() {}

random_mutation_generator::random_mutation_generator(generate_counters counters)
    : _impl(std::make_unique<random_mutation_generator::impl>(counters))
{ }

mutation random_mutation_generator::operator()() {
    return (*_impl)();
}

schema_ptr random_mutation_generator::schema() const {
    return _impl->_schema;
}

clustering_key random_mutation_generator::make_random_key() {
    return _impl->make_random_key();
}

std::vector<query::clustering_range> random_mutation_generator::make_random_ranges(unsigned n_ranges) {
    return _impl->make_random_ranges(n_ranges);
}
