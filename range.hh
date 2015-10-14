/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
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

#include <experimental/optional>
#include <iostream>

// A range which can have inclusive, exclusive or open-ended bounds on each end.
template<typename T>
class range {
    template <typename U>
    using optional = std::experimental::optional<U>;
public:
    class bound {
        T _value;
        bool _inclusive;
    public:
        bound(T value, bool inclusive = true)
            : _value(std::move(value))
            , _inclusive(inclusive)
        { }
        const T& value() const & { return _value; }
        T&& value() && { return std::move(_value); }
        bool is_inclusive() const { return _inclusive; }
        bool operator==(const bound& other) const {
            return (_value == other._value) && (_inclusive == other._inclusive);
        }
    };
private:
    optional<bound> _start;
    optional<bound> _end;
    bool _singular;
public:
    range(optional<bound> start, optional<bound> end, bool singular = false)
        : _start(std::move(start))
        , _end(std::move(end))
        , _singular(singular)
    { }
    range(T value)
        : _start(bound(std::move(value), true))
        , _end()
        , _singular(true)
    { }
    range() : range({}, {}) {}
public:
    // the point is before the range (works only for non wrapped ranges)
    // Comparator must define a total ordering on T.
    template<typename Comparator>
    bool before(const T& point, Comparator&& cmp) const {
        assert(!is_wrap_around(cmp));
        if (!start()) {
            return false; //open start, no points before
        }
        auto r = cmp(point, start()->value());
        if (r < 0) {
            return true;
        }
        if (!start()->is_inclusive() && r == 0) {
            return true;
        }
        return false;
    }
    // the point is after the range (works only for non wrapped ranges)
    // Comparator must define a total ordering on T.
    template<typename Comparator>
    bool after(const T& point, Comparator&& cmp) const {
        assert(!is_wrap_around(cmp));
        if (!end()) {
            return false; //open end, no points after
        }
        auto r = cmp(end()->value(), point);
        if (r < 0) {
            return true;
        }
        if (!end()->is_inclusive() && r == 0) {
            return true;
        }
        return false;
    }
    // check if two ranges overlap.
    template<typename Comparator>
    bool overlap(const range& other, Comparator&& cmp) const {
        bool this_wraps = is_wrap_around(cmp);
        bool other_wraps = other.is_wrap_around(cmp);

        if (this_wraps && other_wraps) {
            return true;
        } else if (this_wraps) {
            auto unwrapped = unwrap();
            return other.overlap(unwrapped.first, cmp) || other.overlap(unwrapped.second, cmp);
        } else if (other_wraps) {
            auto unwrapped = other.unwrap();
            return overlap(unwrapped.first, cmp) || overlap(unwrapped.second, cmp);
        }

        // if both this and other have an open start, the two ranges will overlap.
        // if only range A has an open start, range A and B will overlap if A contains B.
        if (!start() && !other.start()) {
            return true;
        } else if (!start()) {
            return contains(other.start()->value(), cmp);
        } else if (!other.start()) {
            return other.contains(start()->value(), cmp);
        }
        // else, the two ranges will overlap if either contain the start of the other.
        return contains(other.start()->value(), cmp) || other.contains(start()->value(), cmp);
    }
    static range make(bound start, bound end) {
        return range({std::move(start)}, {std::move(end)});
    }
    static range make_open_ended_both_sides() {
        return {{}, {}};
    }
    static range make_singular(T value) {
        return {std::move(value)};
    }
    static range make_starting_with(bound b) {
        return {{std::move(b)}, {}};
    }
    static range make_ending_with(bound b) {
        return {{}, {std::move(b)}};
    }
    bool is_singular() const {
        return _singular;
    }
    bool is_full() const {
        return !_start && !_end;
    }
    void reverse() {
        if (!_singular) {
            std::swap(_start, _end);
        }
    }
    const optional<bound>& start() const {
        return _start;
    }
    const optional<bound>& end() const {
        return _singular ? _start : _end;
    }
    // Range is a wrap around if end value is smaller than the start value
    // or they're equal and at least one bound is not inclusive.
    // Comparator must define a total ordering on T.
    template<typename Comparator>
    bool is_wrap_around(Comparator&& cmp) const {
        if (_end && _start) {
            auto r = cmp(end()->value(), start()->value());
            return r < 0
                   || (r == 0 && (!start()->is_inclusive() || !end()->is_inclusive()));
        } else {
            return false; // open ended range or singular range don't wrap around
        }
    }
    // Converts a wrap-around range to two non-wrap-around ranges.
    // Call only when is_wrap_around().
    std::pair<range, range> unwrap() const {
        return {
            { start(), {} },
            { {}, end() }
        };
    }
    // the point is inside the range
    // Comparator must define a total ordering on T.
    template<typename Comparator>
    bool contains(const T& point, Comparator&& cmp) const {
        if (is_wrap_around(cmp)) {
            auto unwrapped = unwrap();
            return unwrapped.first.contains(point, cmp)
                   || unwrapped.second.contains(point, cmp);
        } else {
            return !before(point, cmp) && !after(point, cmp);
        }
    }
    // Returns true iff all values contained by other are also contained by this.
    // Comparator must define a total ordering on T.
    template<typename Comparator>
    bool contains(const range& other, Comparator&& cmp) const {
        bool this_wraps = is_wrap_around(cmp);
        bool other_wraps = other.is_wrap_around(cmp);

        if (this_wraps && other_wraps) {
            return cmp(start()->value(), other.start()->value())
                   <= -(!start()->is_inclusive() && other.start()->is_inclusive())
                && cmp(end()->value(), other.end()->value())
                   >= (!end()->is_inclusive() && other.end()->is_inclusive());
        }

        if (!this_wraps && !other_wraps) {
            return (!start() || (other.start()
                                 && cmp(start()->value(), other.start()->value())
                                    <= -(!start()->is_inclusive() && other.start()->is_inclusive())))
                && (!end() || (other.end()
                               && cmp(end()->value(), other.end()->value())
                                  >= (!end()->is_inclusive() && other.end()->is_inclusive())));
        }

        if (other_wraps) { // && !this_wraps
            return !start() && !end();
        }

        // !other_wraps && this_wraps
        return (other.start() && cmp(start()->value(), other.start()->value())
                                 <= -(!start()->is_inclusive() && other.start()->is_inclusive()))
                || (other.end() && cmp(end()->value(), other.end()->value())
                                   >= (!end()->is_inclusive() && other.end()->is_inclusive()));
    }
    // split range in two around a split_point. split_point has to be inside the range
    // split_point will belong to first range
    // Comparator must define a total ordering on T.
    template<typename Comparator>
    std::pair<range<T>, range<T>> split(const T& split_point, Comparator&& cmp) const {
        assert(contains(split_point, std::forward<Comparator>(cmp)));
        range left(start(), bound(split_point));
        range right(bound(split_point, false), end());
        return std::make_pair(std::move(left), std::move(right));
    }
    // Create a sub-range including values greater than the split_point. split_point has to be inside the range
    // Comparator must define a total ordering on T.
    template<typename Comparator>
    range<T> split_after(const T& split_point, Comparator&& cmp) const {
        assert(contains(split_point, std::forward<Comparator>(cmp)));
        return range(bound(split_point, false), end());
    }
    // Transforms this range into a new range of a different value type
    // Supplied transformer should transform value of type T (the old type) into value of type U (the new type).
    template<typename U, typename Transformer>
    range<U> transform(Transformer&& transformer) && {
        auto t = [&transformer] (std::experimental::optional<bound>&& b)
            -> std::experimental::optional<typename range<U>::bound>
        {
            if (!b) {
                return {};
            }
            return { { transformer(std::move(*b).value()), b->is_inclusive() } };
        };
        return range<U>(t(std::move(_start)), t(std::move(_end)), _singular);
    }

private:
    template<typename U>
    static auto serialized_size(U& t) -> decltype(t.serialized_size()) {
        return t.serialized_size();
    }
    template<typename U>
    static auto serialized_size(U& t) -> decltype(t.representation().size()) {
        return t.representation().size() + serialize_int32_size;
    }
    template<typename U>
    static auto serialize(bytes::iterator& out, const U& t) -> decltype(t.serialize(out)) {
        return t.serialize(out);
    }
    template<typename U>
    static auto serialize(bytes::iterator& out, const U& t) -> decltype(t.representation(), void()) {
        auto v = t.representation();
        serialize_int32(out, v.size());
        out = std::copy(v.begin(), v.end(), out);
    }
    template<typename U>
    static auto deserialize_type(bytes_view& in) -> decltype(U::deserialize(in)) {
        return U::deserialize(in);
    }
    template<typename U>
    static auto deserialize_type(bytes_view& in) -> decltype(U::from_bytes(bytes())) {
        bytes v;
        uint32_t size = read_simple<uint32_t>(in);
        return U::from_bytes(to_bytes(read_simple_bytes(in, size)));
    }
public:
    size_t serialized_size() const {
        auto bound_size = [this] (const optional<bound>& b) {
            size_t size = serialize_bool_size; // if optional is armed
            if (b) {
                size += serialized_size(b.value().value());
                size += serialize_bool_size; // _inclusive
            }
            return size;
        };
        return serialize_bool_size // singular
                + bound_size(_start) + bound_size(_end);
    }

    void serialize(bytes::iterator& out) const {
        auto serialize_bound = [this] (bytes::iterator& out, const optional<bound>& b) {
            if (b) {
                serialize_bool(out, true);
                serialize(out, b.value().value());
                serialize_bool(out, b.value().is_inclusive());
            } else {
                serialize_bool(out, false);
            }
        };
        serialize_bound(out, _start);
        serialize_bound(out, _end);
        serialize_bool(out, is_singular());
    }

    static range deserialize(bytes_view& in) {
        auto deserialize_bound = [](bytes_view& in) {
            optional<bound> b;
            bool armed = read_simple<bool>(in);
            if (!armed) {
                return b;
            } else {
                T t = deserialize_type<T>(in);
                bool inc = read_simple<bool>(in);
                b.emplace(std::move(t), inc);
            }
            return b;
        };
        auto s = deserialize_bound(in);
        auto e = deserialize_bound(in);
        bool singular = read_simple<bool>(in);
        return range(std::move(s), std::move(e), singular);
    }

    bool operator==(const range& other) const {
        return (_start == other._start) && (_end == other._end) && (_singular == other._singular);
    }

    template<typename U>
    friend std::ostream& operator<<(std::ostream& out, const range<U>& r);
};

template<typename U>
std::ostream& operator<<(std::ostream& out, const range<U>& r) {
    if (r.is_singular()) {
        return out << "==" << r.start()->value();
    }

    if (!r.start()) {
        out << "(-inf, ";
    } else {
        if (r.start()->is_inclusive()) {
            out << "[";
        } else {
            out << "(";
        }
        out << r.start()->value() << ", ";
    }

    if (!r.end()) {
        out << "+inf)";
    } else {
        out << r.end()->value();
        if (r.end()->is_inclusive()) {
            out << "]";
        } else {
            out << ")";
        }
    }

    return out;
}

