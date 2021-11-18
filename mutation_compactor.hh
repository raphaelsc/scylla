/*
 * Copyright (C) 2016-present ScyllaDB
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

#include "compaction/compaction_garbage_collector.hh"
#include "mutation_fragment.hh"
#include "flat_mutation_reader.hh"

static inline bool has_ck_selector(const query::clustering_row_ranges& ranges) {
    // Like PK range, an empty row range, should be considered an "exclude all" restriction
    return ranges.empty() || std::any_of(ranges.begin(), ranges.end(), [](auto& r) {
        return !r.is_full();
    });
}

enum class emit_only_live_rows {
    no,
    yes,
};

enum class compact_for_sstables {
    no,
    yes,
};

template<typename T>
concept CompactedFragmentsConsumer = requires(T obj, tombstone t, const dht::decorated_key& dk, static_row sr,
        clustering_row cr, range_tombstone rt, tombstone current_tombstone, row_tombstone current_row_tombstone, bool is_alive) {
    obj.consume_new_partition(dk);
    obj.consume(t);
    { obj.consume(std::move(sr), current_tombstone, is_alive) } -> std::same_as<stop_iteration>;
    { obj.consume(std::move(cr), current_row_tombstone, is_alive) } -> std::same_as<stop_iteration>;
    { obj.consume(std::move(rt)) } -> std::same_as<stop_iteration>;
    { obj.consume_end_of_partition() } -> std::same_as<stop_iteration>;
    obj.consume_end_of_stream();
};

struct detached_compaction_state {
    ::partition_start partition_start;
    std::optional<::static_row> static_row;
    std::deque<range_tombstone> range_tombstones;
};

class noop_compacted_fragments_consumer {
public:
    void consume_new_partition(const dht::decorated_key& dk) {}
    void consume(tombstone t) {}
    stop_iteration consume(static_row&& sr, tombstone, bool) { return stop_iteration::no; }
    stop_iteration consume(clustering_row&& cr, row_tombstone, bool) { return stop_iteration::no; }
    stop_iteration consume(range_tombstone&& rt) { return stop_iteration::no; }
    stop_iteration consume_end_of_partition() { return stop_iteration::no; }
    void consume_end_of_stream() {}
};

class mutation_compactor_garbage_collector : public compaction_garbage_collector {
    const schema& _schema;
    column_kind _kind;
    std::optional<clustering_key> _ckey;
    row_tombstone _tomb;
    row_marker _marker;
    row _row;

public:
    explicit mutation_compactor_garbage_collector(const schema& schema)
        : _schema(schema) {
    }
    void start_collecting_static_row() {
        _kind = column_kind::static_column;
    }
    void start_collecting_clustering_row(clustering_key ckey) {
        _kind = column_kind::regular_column;
        _ckey = std::move(ckey);
    }
    void collect(row_tombstone tomb) {
        _tomb = tomb;
    }
    virtual void collect(column_id id, atomic_cell cell) override {
        _row.apply(_schema.column_at(_kind, id), std::move(cell));
    }
    virtual void collect(column_id id, collection_mutation_description mut) override {
        if (mut.tomb || !mut.cells.empty()) {
            const auto& cdef = _schema.column_at(_kind, id);
            _row.apply(cdef, mut.serialize(*cdef.type));
        }
    }
    virtual void collect(row_marker marker) override {
        _marker = marker;
    }
    template <typename Consumer>
    void consume_static_row(Consumer&& consumer) {
        if (!_row.empty()) {
            consumer(static_row(std::move(_row)));
            _row = {};
        }
    }
    template <typename Consumer>
    void consume_clustering_row(Consumer&& consumer) {
        if (_tomb || !_marker.is_missing() || !_row.empty()) {
            consumer(clustering_row(std::move(*_ckey), _tomb, _marker, std::move(_row)));
            _ckey.reset();
            _tomb = {};
            _marker = {};
            _row = {};
        }
    }
};

struct compaction_stats {
    struct row_stats {
        uint64_t live = 0;
        uint64_t dead = 0;

        void operator+=(bool is_live) {
            live += is_live;
            dead += !is_live;
        }
        uint64_t total() const {
            return live + dead;
        }
    };

    uint64_t partitions = 0;
    row_stats static_rows;
    row_stats clustering_rows;
    uint64_t range_tombstones = 0;
};

// emit_only_live::yes will cause compact_for_query to emit only live
// static and clustering rows. It doesn't affect the way range tombstones are
// emitted.
template<emit_only_live_rows OnlyLive, compact_for_sstables SSTableCompaction>
class compact_mutation_state {
    const schema& _schema;
    gc_clock::time_point _query_time;
    gc_clock::time_point _gc_before;
    std::function<api::timestamp_type(const dht::decorated_key&)> _get_max_purgeable;
    can_gc_fn _can_gc;
    api::timestamp_type _max_purgeable = api::missing_timestamp;
    const query::partition_slice& _slice;
    uint64_t _row_limit{};
    uint32_t _partition_limit{};
    uint64_t _partition_row_limit{};

    range_tombstone_accumulator _range_tombstones;

    bool _static_row_live{};
    uint64_t _rows_in_current_partition;
    uint32_t _current_partition_limit;
    bool _empty_partition{};
    bool _empty_partition_in_gc_consumer{};
    const dht::decorated_key* _dk{};
    dht::decorated_key _last_dk;
    bool _return_static_content_on_partition_with_no_rows{};

    std::optional<static_row> _last_static_row;

    std::unique_ptr<mutation_compactor_garbage_collector> _collector;

    compaction_stats _stats;
private:
    static constexpr bool only_live() {
        return OnlyLive == emit_only_live_rows::yes;
    }
    static constexpr bool sstable_compaction() {
        return SSTableCompaction == compact_for_sstables::yes;
    }

    template <typename GCConsumer>
    void partition_is_not_empty_for_gc_consumer(GCConsumer& gc_consumer) {
        if (_empty_partition_in_gc_consumer) {
            _empty_partition_in_gc_consumer = false;
            gc_consumer.consume_new_partition(*_dk);
            auto pt = _range_tombstones.get_partition_tombstone();
            if (pt && can_purge_tombstone(pt)) {
                gc_consumer.consume(pt);
            }
        }
    }

    template <typename Consumer>
    void partition_is_not_empty(Consumer& consumer) {
        if (_empty_partition) {
            _empty_partition = false;
            ++_stats.partitions;
            consumer.consume_new_partition(*_dk);
            auto pt = _range_tombstones.get_partition_tombstone();
            if (pt && !can_purge_tombstone(pt)) {
                consumer.consume(pt);
            }
        }
    }

    bool can_purge_tombstone(const tombstone& t) {
        return t.deletion_time < _gc_before && can_gc(t);
    };

    bool can_purge_tombstone(const row_tombstone& t) {
        return t.max_deletion_time() < _gc_before && can_gc(t.tomb());
    };

    bool can_gc(tombstone t) {
        if (!sstable_compaction()) {
            return true;
        }
        if (!t) {
            return false;
        }
        if (_max_purgeable == api::missing_timestamp) {
            _max_purgeable = _get_max_purgeable(*_dk);
        }
        return t.timestamp < _max_purgeable;
    };

public:
    struct parameters {
        static constexpr emit_only_live_rows only_live = OnlyLive;
        static constexpr compact_for_sstables sstable_compaction = SSTableCompaction;
    };

    compact_mutation_state(compact_mutation_state&&) = delete; // Because 'this' is captured

    compact_mutation_state(const schema& s, gc_clock::time_point query_time, const query::partition_slice& slice, uint64_t limit,
              uint32_t partition_limit)
        : _schema(s)
        , _query_time(query_time)
        , _gc_before(saturating_subtract(query_time, s.gc_grace_seconds()))
        , _can_gc(always_gc)
        , _slice(slice)
        , _row_limit(limit)
        , _partition_limit(partition_limit)
        , _partition_row_limit(_slice.options.contains(query::partition_slice::option::distinct) ? 1 : slice.partition_row_limit())
        , _range_tombstones(s)
        , _last_dk({dht::token(), partition_key::make_empty()})
    {
        static_assert(!sstable_compaction(), "This constructor cannot be used for sstable compaction.");
    }

    compact_mutation_state(const schema& s, gc_clock::time_point compaction_time,
            std::function<api::timestamp_type(const dht::decorated_key&)> get_max_purgeable)
        : _schema(s)
        , _query_time(compaction_time)
        , _gc_before(saturating_subtract(_query_time, s.gc_grace_seconds()))
        , _get_max_purgeable(std::move(get_max_purgeable))
        , _can_gc([this] (tombstone t) { return can_gc(t); })
        , _slice(s.full_slice())
        , _range_tombstones(s)
        , _last_dk({dht::token(), partition_key::make_empty()})
        , _collector(std::make_unique<mutation_compactor_garbage_collector>(_schema))
    {
        static_assert(sstable_compaction(), "This constructor can only be used for sstable compaction.");
        static_assert(!only_live(), "SSTable compaction cannot be run with emit_only_live_rows::yes.");
    }

    void consume_new_partition(const dht::decorated_key& dk) {
        auto& pk = dk.key();
        _dk = &dk;
        _return_static_content_on_partition_with_no_rows =
            _slice.options.contains(query::partition_slice::option::always_return_static_content) ||
            !has_ck_selector(_slice.row_ranges(_schema, pk));
        _empty_partition = true;
        _empty_partition_in_gc_consumer = true;
        _rows_in_current_partition = 0;
        _static_row_live = false;
        _range_tombstones.clear();
        _current_partition_limit = std::min(_row_limit, _partition_row_limit);
        _max_purgeable = api::missing_timestamp;
        _last_static_row.reset();
    }

    template <typename Consumer, typename GCConsumer>
    requires CompactedFragmentsConsumer<Consumer> && CompactedFragmentsConsumer<GCConsumer>
    void consume(tombstone t, Consumer& consumer, GCConsumer& gc_consumer) {
        _range_tombstones.set_partition_tombstone(t);
        if (!only_live()) {
            if (can_purge_tombstone(t)) {
                partition_is_not_empty_for_gc_consumer(gc_consumer);
            } else {
                partition_is_not_empty(consumer);
            }
         }
    }

    template <typename Consumer, typename GCConsumer>
    requires CompactedFragmentsConsumer<Consumer> && CompactedFragmentsConsumer<GCConsumer>
    stop_iteration consume(static_row&& sr, Consumer& consumer, GCConsumer& gc_consumer) {
        _last_static_row = static_row(_schema, sr);
        auto current_tombstone = _range_tombstones.get_partition_tombstone();
        if constexpr (sstable_compaction()) {
            _collector->start_collecting_static_row();
        }
        bool is_live = sr.cells().compact_and_expire(_schema, column_kind::static_column, row_tombstone(current_tombstone),
                _query_time, _can_gc, _gc_before, _collector.get());
        _stats.static_rows += is_live;
        if constexpr (sstable_compaction()) {
            _collector->consume_static_row([this, &gc_consumer, current_tombstone] (static_row&& sr_garbage) {
                partition_is_not_empty_for_gc_consumer(gc_consumer);
                // We are passing only dead (purged) data so pass is_live=false.
                gc_consumer.consume(std::move(sr_garbage), current_tombstone, false);
            });
        } else {
            if (can_purge_tombstone(current_tombstone)) {
                current_tombstone = {};
            }
        }
        _static_row_live = is_live;
        if (is_live || (!only_live() && !sr.empty())) {
            partition_is_not_empty(consumer);
            return consumer.consume(std::move(sr), current_tombstone, is_live);
        }
        return stop_iteration::no;
    }

    template <typename Consumer, typename GCConsumer>
    requires CompactedFragmentsConsumer<Consumer> && CompactedFragmentsConsumer<GCConsumer>
    stop_iteration consume(clustering_row&& cr, Consumer& consumer, GCConsumer& gc_consumer) {
        auto current_tombstone = _range_tombstones.tombstone_for_row(cr.key());
        auto t = cr.tomb();
        t.apply(current_tombstone);

        if constexpr (sstable_compaction()) {
            _collector->start_collecting_clustering_row(cr.key());
        }

        {
            const auto rt = cr.tomb();
            if (rt.tomb() <= current_tombstone) {
                cr.remove_tombstone();
            } else if (can_purge_tombstone(rt)) {
                if constexpr (sstable_compaction()) {
                    _collector->collect(rt);
                }
                cr.remove_tombstone();
            }
        }

        bool is_live = cr.marker().compact_and_expire(t.tomb(), _query_time, _can_gc, _gc_before, _collector.get());
        is_live |= cr.cells().compact_and_expire(_schema, column_kind::regular_column, t, _query_time, _can_gc, _gc_before, cr.marker(),
                _collector.get());
        _stats.clustering_rows += is_live;

        if constexpr (sstable_compaction()) {
            _collector->consume_clustering_row([this, &gc_consumer, t] (clustering_row&& cr_garbage) {
                partition_is_not_empty_for_gc_consumer(gc_consumer);
                // We are passing only dead (purged) data so pass is_live=false.
                gc_consumer.consume(std::move(cr_garbage), t, false);
            });
        } else {
            if (can_purge_tombstone(t)) {
                t = {};
            }
        }

        if (only_live() && is_live) {
            partition_is_not_empty(consumer);
            auto stop = consumer.consume(std::move(cr), t, true);
            if (++_rows_in_current_partition == _current_partition_limit) {
                return stop_iteration::yes;
            }
            return stop;
        } else if (!only_live()) {
            auto stop = stop_iteration::no;
            if (!cr.empty()) {
                partition_is_not_empty(consumer);
                stop = consumer.consume(std::move(cr), t, is_live);
            }
            if (!sstable_compaction() && is_live && ++_rows_in_current_partition == _current_partition_limit) {
                return stop_iteration::yes;
            }
            return stop;
        }
        return stop_iteration::no;
    }

    template <typename Consumer, typename GCConsumer>
    requires CompactedFragmentsConsumer<Consumer> && CompactedFragmentsConsumer<GCConsumer>
    stop_iteration consume(range_tombstone&& rt, Consumer& consumer, GCConsumer& gc_consumer) {
        ++_stats.range_tombstones;
        _range_tombstones.apply(rt);
        // FIXME: drop tombstone if it is fully covered by other range tombstones
        if (rt.tomb > _range_tombstones.get_partition_tombstone()) {
            if (can_purge_tombstone(rt.tomb)) {
                partition_is_not_empty_for_gc_consumer(gc_consumer);
                return gc_consumer.consume(std::move(rt));
            } else {
                partition_is_not_empty(consumer);
                return consumer.consume(std::move(rt));
            }
         }
        return stop_iteration::no;
    }

    template <typename Consumer, typename GCConsumer>
    requires CompactedFragmentsConsumer<Consumer> && CompactedFragmentsConsumer<GCConsumer>
    stop_iteration consume_end_of_partition(Consumer& consumer, GCConsumer& gc_consumer) {
        if (!_empty_partition_in_gc_consumer) {
            gc_consumer.consume_end_of_partition();
        }
        if (!_empty_partition) {
            // #589 - Do not add extra row for statics unless we did a CK range-less query.
            // See comment in query
            if (_rows_in_current_partition == 0 && _static_row_live &&
                    _return_static_content_on_partition_with_no_rows) {
                ++_rows_in_current_partition;
            }

            _row_limit -= _rows_in_current_partition;
            _partition_limit -= _rows_in_current_partition > 0;
            auto stop = consumer.consume_end_of_partition();
            if (!sstable_compaction()) {
                return _row_limit && _partition_limit && stop != stop_iteration::yes
                       ? stop_iteration::no : stop_iteration::yes;
            }
        }
        return stop_iteration::no;
    }

    template <typename Consumer, typename GCConsumer>
    requires CompactedFragmentsConsumer<Consumer> && CompactedFragmentsConsumer<GCConsumer>
    auto consume_end_of_stream(Consumer& consumer, GCConsumer& gc_consumer) {
        if (_dk) {
            _last_dk = *_dk;
            _dk = &_last_dk;
        }
        if constexpr (std::is_same_v<std::result_of_t<decltype(&GCConsumer::consume_end_of_stream)(GCConsumer&)>, void>) {
            gc_consumer.consume_end_of_stream();
            return consumer.consume_end_of_stream();
        } else {
            return std::pair(consumer.consume_end_of_stream(), gc_consumer.consume_end_of_stream());
        }
    }

    /// The decorated key of the partition the compaction is positioned in.
    /// Can be null if the compaction wasn't started yet.
    const dht::decorated_key* current_partition() const {
        return _dk;
    }

    /// Reset limits and query-time to the new page's ones and re-emit the
    /// partition-header and static row if there are clustering rows or range
    /// tombstones left in the partition.
    template <typename Consumer>
    requires CompactedFragmentsConsumer<Consumer>
    void start_new_page(uint64_t row_limit,
            uint32_t partition_limit,
            gc_clock::time_point query_time,
            mutation_fragment::kind next_fragment_kind,
            Consumer& consumer) {
        _empty_partition = true;
        _static_row_live = false;
        _row_limit = row_limit;
        _partition_limit = partition_limit;
        _rows_in_current_partition = 0;
        _current_partition_limit = std::min(_row_limit, _partition_row_limit);
        _query_time = query_time;
        _gc_before = saturating_subtract(query_time, _schema.gc_grace_seconds());
        _stats = {};

        if ((next_fragment_kind == mutation_fragment::kind::clustering_row || next_fragment_kind == mutation_fragment::kind::range_tombstone)
                && _last_static_row) {
            // Stopping here would cause an infinite loop so ignore return value.
            noop_compacted_fragments_consumer nc;
            consume(*std::exchange(_last_static_row, {}), consumer, nc);
        }
    }

    bool are_limits_reached() const {
        return _row_limit == 0 || _partition_limit == 0;
    }

    /// Detach the internal state of the compactor
    ///
    /// The state is represented by the last seen partition header, static row
    /// and active range tombstones. Replaying these fragments through a new
    /// compactor will result in the new compactor being in the same state *this
    /// is (given the same outside parameters of course). Practically this
    /// allows the compaction state to be stored in the compacted reader.
    detached_compaction_state detach_state() && {
        partition_start ps(std::move(_last_dk), _range_tombstones.get_partition_tombstone());
        return {std::move(ps), std::move(_last_static_row), std::move(_range_tombstones).range_tombstones()};
    }

    const compaction_stats& stats() const { return _stats; }
};

template<emit_only_live_rows OnlyLive, compact_for_sstables SSTableCompaction, typename Consumer, typename GCConsumer>
requires CompactedFragmentsConsumer<Consumer> && CompactedFragmentsConsumer<GCConsumer>
class compact_mutation {
    lw_shared_ptr<compact_mutation_state<OnlyLive, SSTableCompaction>> _state;
    Consumer _consumer;
    // Garbage Collected Consumer
    GCConsumer _gc_consumer;

public:
    compact_mutation(const schema& s, gc_clock::time_point query_time, const query::partition_slice& slice, uint64_t limit,
              uint32_t partition_limit, Consumer consumer, GCConsumer gc_consumer = GCConsumer())
        : _state(make_lw_shared<compact_mutation_state<OnlyLive, SSTableCompaction>>(s, query_time, slice, limit, partition_limit))
        , _consumer(std::move(consumer))
        , _gc_consumer(std::move(gc_consumer)) {
    }

    compact_mutation(const schema& s, gc_clock::time_point compaction_time,
            std::function<api::timestamp_type(const dht::decorated_key&)> get_max_purgeable, Consumer consumer, GCConsumer gc_consumer = GCConsumer())
        : _state(make_lw_shared<compact_mutation_state<OnlyLive, SSTableCompaction>>(s, compaction_time, get_max_purgeable))
        , _consumer(std::move(consumer))
        , _gc_consumer(std::move(gc_consumer)) {
    }

    compact_mutation(lw_shared_ptr<compact_mutation_state<OnlyLive, SSTableCompaction>> state, Consumer consumer,
                     GCConsumer gc_consumer = GCConsumer())
        : _state(std::move(state))
        , _consumer(std::move(consumer))
        , _gc_consumer(std::move(gc_consumer)) {
    }

    void consume_new_partition(const dht::decorated_key& dk) {
        _state->consume_new_partition(dk);
    }

    void consume(tombstone t) {
        _state->consume(std::move(t), _consumer, _gc_consumer);
    }

    stop_iteration consume(static_row&& sr) {
        return _state->consume(std::move(sr), _consumer, _gc_consumer);
    }

    stop_iteration consume(clustering_row&& cr) {
        return _state->consume(std::move(cr), _consumer, _gc_consumer);
    }

    stop_iteration consume(range_tombstone&& rt) {
        return _state->consume(std::move(rt), _consumer, _gc_consumer);
    }

    stop_iteration consume_end_of_partition() {
        return _state->consume_end_of_partition(_consumer, _gc_consumer);
    }

    auto consume_end_of_stream() {
        return _state->consume_end_of_stream(_consumer, _gc_consumer);
    }
};

template<emit_only_live_rows only_live, typename Consumer>
requires CompactedFragmentsConsumer<Consumer>
struct compact_for_query : compact_mutation<only_live, compact_for_sstables::no, Consumer, noop_compacted_fragments_consumer> {
    using compact_mutation<only_live, compact_for_sstables::no, Consumer, noop_compacted_fragments_consumer>::compact_mutation;
};

template<emit_only_live_rows OnlyLive>
using compact_for_query_state = compact_mutation_state<OnlyLive, compact_for_sstables::no>;

template<typename Consumer, typename GCConsumer = noop_compacted_fragments_consumer>
requires CompactedFragmentsConsumer<Consumer> && CompactedFragmentsConsumer<GCConsumer>
struct compact_for_compaction : compact_mutation<emit_only_live_rows::no, compact_for_sstables::yes, Consumer, GCConsumer> {
    using compact_mutation<emit_only_live_rows::no, compact_for_sstables::yes, Consumer, GCConsumer>::compact_mutation;
};

class compacting_reader : public flat_mutation_reader::impl {
    friend class compact_mutation_state<emit_only_live_rows::no, compact_for_sstables::yes>;

private:
    flat_mutation_reader _reader;
    compact_mutation_state<emit_only_live_rows::no, compact_for_sstables::yes> _compactor;
    noop_compacted_fragments_consumer _gc_consumer;

    // Uncompacted stream
    partition_start _last_uncompacted_partition_start;
    mutation_fragment::kind _last_uncompacted_kind = mutation_fragment::kind::partition_end;

    // Compacted stream
    bool _has_compacted_partition_start = false;
    bool _ignore_partition_end = false;

private:
    void maybe_push_partition_start() {
        if (_has_compacted_partition_start) {
            push_mutation_fragment(mutation_fragment(*_schema, _permit, std::move(_last_uncompacted_partition_start)));
            _has_compacted_partition_start = false;
        }
    }
    void maybe_inject_partition_end() {
        // The compactor needs a valid stream, but downstream doesn't care about
        // the injected partition end, so ignore it.
        if (_last_uncompacted_kind != mutation_fragment::kind::partition_end) {
            _ignore_partition_end = true;
            _compactor.consume_end_of_partition(*this, _gc_consumer);
            _ignore_partition_end = false;
        }
    }
    void consume_new_partition(const dht::decorated_key& dk) {
        _has_compacted_partition_start = true;
        // We need to reset the partition's tombstone here. If the tombstone is
        // compacted away, `consume(tombstone)` below is simply not called. If
        // it is not compacted away, `consume(tombstone)` below will restore it.
        _last_uncompacted_partition_start.partition_tombstone() = {};
    }
    void consume(tombstone t) {
        _last_uncompacted_partition_start.partition_tombstone() = t;
        maybe_push_partition_start();
    }
    stop_iteration consume(static_row&& sr, tombstone, bool) {
        maybe_push_partition_start();
        push_mutation_fragment(mutation_fragment(*_schema, _permit, std::move(sr)));
        return stop_iteration::no;
    }
    stop_iteration consume(clustering_row&& cr, row_tombstone, bool) {
        maybe_push_partition_start();
        push_mutation_fragment(mutation_fragment(*_schema, _permit, std::move(cr)));
        return stop_iteration::no;
    }
    stop_iteration consume(range_tombstone&& rt) {
        maybe_push_partition_start();
        push_mutation_fragment(mutation_fragment(*_schema, _permit, std::move(rt)));
        return stop_iteration::no;
    }
    stop_iteration consume_end_of_partition() {
        maybe_push_partition_start();
        if (!_ignore_partition_end) {
            push_mutation_fragment(mutation_fragment(*_schema, _permit, partition_end{}));
        }
        return stop_iteration::no;
    }
    void consume_end_of_stream() {
    }

public:
    compacting_reader(flat_mutation_reader source, gc_clock::time_point compaction_time,
                      std::function<api::timestamp_type(const dht::decorated_key&)> get_max_purgeable)
            : impl(source.schema(), source.permit())
            , _reader(std::move(source))
            , _compactor(*_schema, compaction_time, get_max_purgeable)
            , _last_uncompacted_partition_start(dht::decorated_key(dht::minimum_token(), partition_key::make_empty()), tombstone{}) {
    }
    virtual future<> fill_buffer() override {
        return do_until([this] { return is_end_of_stream() || is_buffer_full(); }, [this] {
            return _reader.fill_buffer().then([this] {
                if (_reader.is_buffer_empty()) {
                    _end_of_stream = _reader.is_end_of_stream();
                }
                // It is important to not consume more than we actually need.
                // Doing so leads to corner cases around `next_partition()`. The
                // fragments consumed after our buffer is full might not be
                // emitted by the compactor, so on a following `next_partition()`
                // call we won't be able to determine whether we are at a
                // partition boundary or not and thus whether we need to forward
                // it to the underlying reader or not.
                // This problem doesn't exist when we want more fragments, in this
                // case we'll keep reading until the compactor emits something or
                // we read EOS, and thus we'll know where we are.
                while (!_reader.is_buffer_empty() && !is_buffer_full()) {
                    auto mf = _reader.pop_mutation_fragment();
                    _last_uncompacted_kind = mf.mutation_fragment_kind();
                    switch (mf.mutation_fragment_kind()) {
                        case mutation_fragment::kind::static_row:
                            _compactor.consume(std::move(mf).as_static_row(), *this, _gc_consumer);
                            break;
                        case mutation_fragment::kind::clustering_row:
                            _compactor.consume(std::move(mf).as_clustering_row(), *this, _gc_consumer);
                            break;
                        case mutation_fragment::kind::range_tombstone:
                            _compactor.consume(std::move(mf).as_range_tombstone(), *this, _gc_consumer);
                            break;
                        case mutation_fragment::kind::partition_start:
                            _last_uncompacted_partition_start = std::move(mf).as_partition_start();
                            _compactor.consume_new_partition(_last_uncompacted_partition_start.key());
                            if (_last_uncompacted_partition_start.partition_tombstone()) {
                                _compactor.consume(_last_uncompacted_partition_start.partition_tombstone(), *this, _gc_consumer);
                            }
                            break;
                        case mutation_fragment::kind::partition_end:
                            _compactor.consume_end_of_partition(*this, _gc_consumer);
                            break;
                    }
                }
            });
        });
    }
    virtual future<> next_partition() override {
        clear_buffer_to_next_partition();
        if (!is_buffer_empty()) {
            return make_ready_future<>();
        }
        _end_of_stream = false;
        maybe_inject_partition_end();
        return _reader.next_partition();
    }
    virtual future<> fast_forward_to(const dht::partition_range& pr) override {
        clear_buffer();
        _end_of_stream = false;
        maybe_inject_partition_end();
        return _reader.fast_forward_to(pr);
    }
    virtual future<> fast_forward_to(position_range pr) override {
        return make_exception_future<>(make_backtraced_exception_ptr<std::bad_function_call>());
    }
    virtual future<> close() noexcept override {
        return _reader.close();
    }
};
