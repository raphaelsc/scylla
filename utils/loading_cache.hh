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

#include <chrono>
#include <unordered_map>
#include <boost/intrusive/unordered_set.hpp>

#include <seastar/core/timer.hh>

#include "utils/exceptions.hh"

namespace bi = boost::intrusive;

namespace utils {
// Simple variant of the "LoadingCache" used for permissions in origin.

typedef lowres_clock loading_cache_clock_type;

template<typename _Tp, typename _Key, typename _Hash, typename _EqualPred>
class timestamped_val : public bi::unordered_set_base_hook<bi::store_hash<true>> {
public:
    typedef _Key key_type;
    typedef _Tp value_type;

private:
    std::experimental::optional<_Tp> _opt_value;
    loading_cache_clock_type::time_point _loaded;
    loading_cache_clock_type::time_point _last_read;
    _Key _key;

public:
    struct key_eq {
       bool operator()(const _Key& k, const timestamped_val& c) const {
           return _EqualPred()(k, c.key());
       }

       bool operator()(const timestamped_val& c, const _Key& k) const {
           return _EqualPred()(c.key(), k);
       }
    };

    timestamped_val(const _Key& key)
        : _loaded(loading_cache_clock_type::now())
        , _last_read(_loaded)
        , _key(key) {}

    timestamped_val(_Key&& key)
        : _loaded(loading_cache_clock_type::now())
        , _last_read(_loaded)
        , _key(std::move(key)) {}

    timestamped_val(const timestamped_val&) = default;
    timestamped_val(timestamped_val&&) = default;

    // Make sure copy/move-assignments don't go through the template below
    timestamped_val& operator=(const timestamped_val&) = default;
    timestamped_val& operator=(timestamped_val&) = default;
    timestamped_val& operator=(timestamped_val&&) = default;

    template <typename U>
    timestamped_val& operator=(U&& new_val) {
        _opt_value = std::forward<U>(new_val);
        _loaded = loading_cache_clock_type::now();
        return *this;
    }

    const _Tp& value() {
        _last_read = loading_cache_clock_type::now();
        return _opt_value.value();
    }

    explicit operator bool() const noexcept {
        return bool(_opt_value);
    }

    loading_cache_clock_type::time_point last_read() const noexcept {
        return _last_read;
    }

    loading_cache_clock_type::time_point loaded() const noexcept {
        return _loaded;
    }

    const _Key& key() const {
        return _key;
    }

    friend bool operator==(const timestamped_val& a, const timestamped_val& b){
        return _EqualPred()(a.key(), b.key());
    }

    friend std::size_t hash_value(const timestamped_val& v) {
        return _Hash()(v.key());
    }
};

class shared_mutex {
private:
    lw_shared_ptr<semaphore> _mutex_ptr;

public:
    shared_mutex() : _mutex_ptr(make_lw_shared<semaphore>(1)) {}
    semaphore& get() const noexcept {
        return *_mutex_ptr;
    }
};

template<typename _Key,
         typename _Tp,
         typename _Hash = std::hash<_Key>,
         typename _Pred = std::equal_to<_Key>,
         typename _Alloc = std::allocator<timestamped_val<_Tp, _Key, _Hash, _Pred>>,
         typename SharedMutexMapAlloc = std::allocator<std::pair<const _Key, shared_mutex>>>
class loading_cache {
private:
    typedef timestamped_val<_Tp, _Key, _Hash, _Pred> ts_value_type;
    typedef bi::unordered_set<ts_value_type, bi::power_2_buckets<true>, bi::compare_hash<true>> set_type;
    typedef std::unordered_map<_Key, shared_mutex, _Hash, _Pred, SharedMutexMapAlloc> write_mutex_map_type;
    typedef loading_cache<_Key, _Tp, _Hash, _Pred, _Alloc> _MyType;
    typedef typename set_type::bucket_traits bi_set_bucket_traits;

    static constexpr int initial_num_buckets = 256;
    static constexpr int max_num_buckets = 1024 * 1024;

public:
    typedef _Tp value_type;
    typedef _Key key_type;
    typedef typename set_type::iterator iterator;

    template<typename Func>
    loading_cache(size_t max_size, std::chrono::milliseconds expiry, std::chrono::milliseconds refresh, logging::logger& logger, Func&& load)
                : _buckets(initial_num_buckets)
                , _set(bi_set_bucket_traits(_buckets.data(), _buckets.size()))
                , _max_size(max_size)
                , _expiry(expiry)
                , _refresh(refresh)
                , _logger(logger)
                , _load(std::forward<Func>(load)) {

        // If expiration period is zero - caching is disabled
        if (!caching_enabled()) {
            return;
        }

        // Sanity check: if expiration period is given then non-zero refresh period and maximal size are required
        if (_refresh == std::chrono::milliseconds(0) || _max_size == 0) {
            throw exceptions::configuration_exception("loading_cache: caching is enabled but refresh period and/or max_size are zero");
        }

        _timer.set_callback([this] { on_timer(); });
        _timer.arm(_refresh);
    }

    ~loading_cache() {
        _set.clear_and_dispose([] (ts_value_type* ptr) { loading_cache::destroy_ts_value(ptr); });
    }

    future<_Tp> get(const _Key& k) {
        // If caching is disabled - always load in the foreground
        if (!caching_enabled()) {
            return _load(k);
        }

        // If the key is not in the cache yet, then find_or_create() is going to
        // create a new uninitialized value in the map. If the value is already
        // in the cache (the fast path) simply return the value. Otherwise, take
        // the mutex and try to load the value (the slow path).
        iterator ts_value_it = find_or_create(k);
        if (*ts_value_it) {
            return make_ready_future<_Tp>(ts_value_it->value());
        } else {
            return slow_load(k);
        }
    }

private:
    bool caching_enabled() const {
        return _expiry != std::chrono::milliseconds(0);
    }

    /// Look for the entry with the given key. It it doesn't exist - create a new one and add it to the _set.
    ///
    /// \param k The key to look for
    ///
    /// \return An iterator to the value with the given key (always dirrerent from _set.end())
    template <typename KeyType>
    iterator find_or_create(KeyType&& k) {
        iterator i = _set.find(k, _Hash(), typename ts_value_type::key_eq());
        if (i == _set.end()) {
            ts_value_type* new_ts_val = _Alloc().allocate(1);
            new(new_ts_val) ts_value_type(std::forward<KeyType>(k));
            auto p = _set.insert(*new_ts_val);
            i = p.first;
        }

        return i;
    }

    static void destroy_ts_value(ts_value_type* val) {
        val->~ts_value_type();
        _Alloc().deallocate(val, 1);
    }

    future<_Tp> slow_load(const _Key& k) {
        // If the key is not in the cache yet, then _write_mutex_map[k] is going
        // to create a new value with the initialized mutex. The mutex is going
        // to serialize the producers and only the first one is going to
        // actually issue a load operation and initialize the value with the
        // received result. The rest are going to see (and read) the initialized
        // value when they enter the critical section.
        shared_mutex sm = _write_mutex_map[k];
        return with_semaphore(sm.get(), 1, [this, k] {
            iterator ts_value_it = find_or_create(k);
            if (*ts_value_it) {
                return make_ready_future<_Tp>(ts_value_it->value());
            }
            _logger.trace("{}: storing the value for the first time", k);
            return _load(k).then([this, k] (_Tp t) {
                // we have to "re-read" the _set here because the value may have been evicted by now
                iterator ts_value_it = find_or_create(std::move(k));
                *ts_value_it = std::move(t);
                return make_ready_future<_Tp>(ts_value_it->value());
            });
        }).finally([sm] {});
    }

    future<> reload(ts_value_type& ts_val) {
        return _load(ts_val.key()).then_wrapped([this, &ts_val] (auto&& f) {
            // The exceptions are related to the load operation itself.
            // We should ignore them for the background reads - if
            // they persist the value will age and will be reloaded in
            // the forground. If the foreground READ fails the error
            // will be propagated up to the user and will fail the
            // corresponding query.
            try {
                ts_val = f.get0();
            } catch (std::exception& e) {
                _logger.debug("{}: reload failed: {}", ts_val.key(), e.what());
            } catch (...) {
                _logger.debug("{}: reload failed: unknown error", ts_val.key());
            }
        });
    }

    void erase(iterator it) {
        _set.erase_and_dispose(it, [] (ts_value_type* ptr) { loading_cache::destroy_ts_value(ptr); });
    }

    // We really miss the std::erase_if()... :(
    void drop_expired() {
        auto now = loading_cache_clock_type::now();
        auto i = _set.begin();
        auto e = _set.end();

        while (i != e) {
            // An entry should be discarded if it hasn't been reloaded for too long or nobody cares about it anymore
            auto since_last_read = now - i->last_read();
            auto since_loaded = now - i->loaded();
            if (_expiry < since_last_read || _expiry < since_loaded) {
                using namespace std::chrono;
                _logger.trace("drop_expired(): {}: dropping the entry: _expiry {},  ms passed since: loaded {} last_read {}", i->key(), _expiry.count(), duration_cast<milliseconds>(since_loaded).count(), duration_cast<milliseconds>(since_last_read).count());
                erase(i++);
                continue;
            }
            ++i;
        }
    }

    // Shrink the cache to the _max_size discarding the least recently used items
    void shrink() {
        if (_max_size != 0 && _set.size() > _max_size) {
            std::vector<iterator> tmp;
            tmp.reserve(_set.size());

            iterator i = _set.begin();
            while (i != _set.end()) {
                tmp.emplace_back(i++);
            }

            std::sort(tmp.begin(), tmp.end(), [] (iterator i1, iterator i2) {
               return i1->last_read() < i2->last_read();
            });

            tmp.resize(_set.size() - _max_size);
            std::for_each(tmp.begin(), tmp.end(), [this] (auto& k) {
                using namespace std::chrono;
                _logger.trace("shrink(): {}: dropping the entry: ms since last_read {}", k->key(), duration_cast<milliseconds>(loading_cache_clock_type::now() - k->last_read()).count());
                this->erase(k);
            });
        }
    }

    void rehash() {
        size_t new_buckets_count = 0;

        // Don't grow or shrink too fast even if there is a steep drop/growth in the number of elements in the set.
        // Exponential growth/backoff should be good enough.
        //
        // Try to keep the load factor between 0.25 and 1.0.
        if (_set.size() < _current_buckets_count / 4) {
            new_buckets_count = _current_buckets_count / 4;
        } else if (_set.size() > _current_buckets_count) {
            new_buckets_count = _current_buckets_count * 2;
        }

        if (new_buckets_count < initial_num_buckets || new_buckets_count > max_num_buckets) {
            return;
        }

        std::vector<typename set_type::bucket_type> new_buckets(new_buckets_count);
        _set.rehash(bi_set_bucket_traits(new_buckets.data(), new_buckets.size()));
        _logger.trace("rehash(): buckets count changed: {} -> {}", _current_buckets_count, new_buckets_count);

        _buckets.swap(new_buckets);
        _current_buckets_count = new_buckets_count;
    }

    void on_timer() {
        _logger.trace("on_timer(): start");

        auto timer_start_tp = loading_cache_clock_type::now();

        // Clear all cached mutexes
        _write_mutex_map.clear();

        // Clean up items that were not touched for the whole _expiry period.
        drop_expired();

        // Remove the least recently used items if map is too big.
        shrink();

        // check if rehashing is needed and do it if it is.
        rehash();

        // Reload all those which vlaue needs to be reloaded.
        parallel_for_each(_set.begin(), _set.end(), [this, curr_time = timer_start_tp] (auto& ts_val) {
            _logger.trace("on_timer(): {}: checking the value age", ts_val.key());
            if (ts_val && ts_val.loaded() + _refresh < curr_time) {
                _logger.trace("on_timer(): {}: reloading the value", ts_val.key());
                return this->reload(ts_val);
            }
            return now();
        }).finally([this, timer_start_tp] {
            _logger.trace("on_timer(): rearming");
            _timer.arm(timer_start_tp + _refresh);
        });
    }

    std::vector<typename set_type::bucket_type> _buckets;
    size_t _current_buckets_count = initial_num_buckets;
    set_type _set;
    write_mutex_map_type _write_mutex_map;
    size_t _max_size;
    std::chrono::milliseconds _expiry;
    std::chrono::milliseconds _refresh;
    logging::logger& _logger;
    std::function<future<_Tp>(const _Key&)> _load;
    timer<lowres_clock> _timer;
};

}

