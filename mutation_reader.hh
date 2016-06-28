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

#pragma once

#include <vector>

#include "mutation.hh"
#include "core/future.hh"
#include "core/future-util.hh"
#include "core/do_with.hh"

// A mutation_reader is an object which allows iterating on mutations: invoke
// the function to get a future for the next mutation, with an unset optional
// marking the end of iteration. After calling mutation_reader's operator(),
// caller must keep the object alive until the returned future is fulfilled.
//
// The mutations returned have strictly monotonically increasing keys. Two
// consecutive mutations never have equal keys.
//
// TODO: When iterating over mutations, we don't need a schema_ptr for every
// single one as it is normally the same for all of them. So "mutation" might
// not be the optimal object to use here.
class mutation_reader final {
public:
    class impl {
    public:
        virtual ~impl() {}
        virtual future<streamed_mutation_opt> operator()() = 0;
    };
private:
    class null_impl final : public impl {
    public:
        virtual future<streamed_mutation_opt> operator()() override { throw std::bad_function_call(); }
    };
private:
    std::unique_ptr<impl> _impl;
public:
    mutation_reader(std::unique_ptr<impl> impl) noexcept : _impl(std::move(impl)) {}
    mutation_reader() : mutation_reader(std::make_unique<null_impl>()) {}
    mutation_reader(mutation_reader&&) = default;
    mutation_reader(const mutation_reader&) = delete;
    mutation_reader& operator=(mutation_reader&&) = default;
    mutation_reader& operator=(const mutation_reader&) = delete;
    future<streamed_mutation_opt> operator()() { return _impl->operator()(); }
};

// Impl: derived from mutation_reader::impl; Args/args: arguments for Impl's constructor
template <typename Impl, typename... Args>
inline
mutation_reader
make_mutation_reader(Args&&... args) {
    return mutation_reader(std::make_unique<Impl>(std::forward<Args>(args)...));
}

// Creates a mutation reader which combines data return by supplied readers.
// Returns mutation of the same schema only when all readers return mutations
// of the same schema.
mutation_reader make_combined_reader(std::vector<mutation_reader>);
mutation_reader make_combined_reader(mutation_reader&& a, mutation_reader&& b);
// reads from the input readers, in order
mutation_reader make_reader_returning(mutation);
mutation_reader make_reader_returning(streamed_mutation);
mutation_reader make_reader_returning_many(std::vector<mutation>,
    query::clustering_key_filtering_context filter = query::no_clustering_key_filtering);
mutation_reader make_reader_returning_many(std::vector<streamed_mutation>);
mutation_reader make_empty_reader();

struct restricted_mutation_reader_config {
    semaphore* sem = nullptr;
    std::chrono::nanoseconds timeout = {};
    size_t max_queue_length = std::numeric_limits<size_t>::max();
};

// Restricts a given `mutation_reader` to a concurrency limited by a `semaphore`.
mutation_reader make_restricted_reader(const restricted_mutation_reader_config& config, unsigned weight, mutation_reader&& base);

/*
template<typename T>
concept bool StreamedMutationFilter() {
    return requires(T t, const streamed_mutation& sm) {
        { t(sm) } -> bool;
    };
}
*/
template <typename MutationFilter>
class filtering_reader : public mutation_reader::impl {
    mutation_reader _rd;
    MutationFilter _filter;
    streamed_mutation_opt _current;
    static_assert(std::is_same<bool, std::result_of_t<MutationFilter(const streamed_mutation&)>>::value, "bad MutationFilter signature");
public:
    filtering_reader(mutation_reader rd, MutationFilter&& filter)
            : _rd(std::move(rd)), _filter(std::forward<MutationFilter>(filter)) {
    }
    virtual future<streamed_mutation_opt> operator()() override {\
        return repeat([this] {
            return _rd().then([this] (streamed_mutation_opt&& mo) mutable {
                if (!mo) {
                    _current = std::move(mo);
                    return stop_iteration::yes;
                } else {
                    if (_filter(*mo)) {
                        _current = std::move(mo);
                        return stop_iteration::yes;
                    }
                    return stop_iteration::no;
                }
            });
        }).then([this] {
            return make_ready_future<streamed_mutation_opt>(std::move(_current));
        });
    };
};

// Creates a mutation_reader wrapper which creates a new stream of mutations
// with some mutations removed from the original stream.
// MutationFilter is a callable which decides which mutations are dropped. It
// accepts mutation const& and returns a bool. The mutation stays in the
// stream if and only if the filter returns true.
template <typename MutationFilter>
mutation_reader make_filtering_reader(mutation_reader rd, MutationFilter&& filter) {
    return make_mutation_reader<filtering_reader<MutationFilter>>(std::move(rd), std::forward<MutationFilter>(filter));
}

// Calls the consumer for each element of the reader's stream until end of stream
// is reached or the consumer requests iteration to stop by returning stop_iteration::yes.
// The consumer should accept mutation as the argument and return stop_iteration.
// The returned future<> resolves when consumption ends.
template <typename Consumer>
inline
future<> consume(mutation_reader& reader, Consumer consumer) {
    static_assert(std::is_same<future<stop_iteration>, futurize_t<std::result_of_t<Consumer(mutation&&)>>>::value, "bad Consumer signature");
    using futurator = futurize<std::result_of_t<Consumer(mutation&&)>>;

    return do_with(std::move(consumer), [&reader] (Consumer& c) -> future<> {
        return repeat([&reader, &c] () {
            return reader().then([] (auto sm) {
                return mutation_from_streamed_mutation(std::move(sm));
            }).then([&c] (mutation_opt&& mo) -> future<stop_iteration> {
                if (!mo) {
                    return make_ready_future<stop_iteration>(stop_iteration::yes);
                }
                return futurator::apply(c, std::move(*mo));
            });
        });
    });
}

// mutation_source represents source of data in mutation form. The data source
// can be queried multiple times and in parallel. For each query it returns
// independent mutation_reader.
// The reader returns mutations having all the same schema, the one passed
// when invoking the source.
class mutation_source {
    using partition_range = const query::partition_range&;
    using clustering_filter = query::clustering_key_filtering_context;
    using io_priority = const io_priority_class&;
    std::function<mutation_reader(schema_ptr, partition_range, clustering_filter, io_priority)> _fn;
public:
    mutation_source(std::function<mutation_reader(schema_ptr, partition_range, clustering_filter, io_priority)> fn)
        : _fn(std::move(fn)) {}
    mutation_source(std::function<mutation_reader(schema_ptr, partition_range, clustering_filter)> fn)
        : _fn([fn = std::move(fn)] (schema_ptr s, partition_range range, clustering_filter ck_filtering, io_priority) {
            return fn(s, range, ck_filtering);
        }) {}
    mutation_source(std::function<mutation_reader(schema_ptr, partition_range range)> fn)
        : _fn([fn = std::move(fn)] (schema_ptr s, partition_range range, clustering_filter, io_priority) {
            return fn(s, range);
        }) {}

    mutation_reader operator()(schema_ptr s, partition_range range, clustering_filter ck_filtering, io_priority pc) const {
        return _fn(std::move(s), range, ck_filtering, pc);
    }
    mutation_reader operator()(schema_ptr s, partition_range range, clustering_filter ck_filtering) const {
        return _fn(std::move(s), range, ck_filtering, default_priority_class());
    }
    mutation_reader operator()(schema_ptr s, partition_range range) const {
        return _fn(std::move(s), range, query::no_clustering_key_filtering, default_priority_class());
    }
};

/// A partition_presence_checker quickly returns whether a key is known not to exist
/// in a data source (it may return false positives, but not false negatives).
enum class partition_presence_checker_result {
    definitely_doesnt_exist,
    maybe_exists
};
using partition_presence_checker = std::function<partition_presence_checker_result (const partition_key& key)>;

inline
partition_presence_checker make_default_partition_presence_checker() {
    return [] (partition_key_view key) { return partition_presence_checker_result::maybe_exists; };
}

template<typename Consumer>
future<stop_iteration> do_consume_streamed_mutation_flattened(streamed_mutation& sm, Consumer& c)
{
    do {
        if (sm.is_buffer_empty()) {
            if (sm.is_end_of_stream()) {
                break;
            }
            auto f = sm.fill_buffer();
            if (!f.available()) {
                return f.then([&] { return do_consume_streamed_mutation_flattened(sm, c); });
            }
            f.get();
        } else {
            if (sm.pop_mutation_fragment().consume(c) == stop_iteration::yes) {
                break;
            }
        }
    } while (true);
    return make_ready_future<stop_iteration>(c.consume_end_of_partition());
}

/*
template<typename T>
concept bool FlattenedConsumer() {
    return StreamedMutationConsumer() && requires(T obj, const partition_key& pk) {
        obj.consume_new_partition(pk);
        obj.consume_end_of_partition();
    };
}
*/
template<typename Consumer>
auto consume_flattened(mutation_reader mr, Consumer c, bool reverse_mutations = false)
{
    return do_with(std::move(mr), std::move(c), stdx::optional<streamed_mutation>(), [reverse_mutations] (auto& mr, auto& c, auto& sm) {
        return repeat([&, reverse_mutations] {
            return mr().then([&, reverse_mutations] (auto smopt) {
                if (!smopt) {
                    return make_ready_future<stop_iteration>(stop_iteration::yes);
                }
                if (!reverse_mutations) {
                    sm.emplace(std::move(*smopt));
                } else {
                    sm.emplace(reverse_streamed_mutation(std::move(*smopt)));
                }
                c.consume_new_partition(sm->key());
                if (sm->partition_tombstone()) {
                    c.consume(sm->partition_tombstone());
                }
                return do_consume_streamed_mutation_flattened(*sm, c);
            });
        }).then([&] {
            return c.consume_end_of_stream();
        });
    });
}

