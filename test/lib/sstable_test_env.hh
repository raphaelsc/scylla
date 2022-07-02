/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/do_with.hh>
#include <seastar/util/noncopyable_function.hh>
#include <seastar/core/sharded.hh>
#include <seastar/util/defer.hh>

#include "sstables/sstables.hh"
#include "test/lib/tmpdir.hh"
#include "test/lib/test_services.hh"
#include "test/lib/log.hh"

namespace sstables {

class test_env_sstables_manager : public sstables_manager {
    using sstables_manager::sstables_manager;
public:
    virtual sstable_writer_config configure_writer(sstring origin = "test") const override {
        return sstables_manager::configure_writer(std::move(origin));
    }
};

class test_env {
    std::unique_ptr<cache_tracker> _cache_tracker;
    std::unique_ptr<test_env_sstables_manager> _mgr;
    std::unique_ptr<reader_concurrency_semaphore> _semaphore;
    const db::config& _db_config;
public:
    explicit test_env()
        : _cache_tracker(std::make_unique<cache_tracker>())
        , _mgr(std::make_unique<test_env_sstables_manager>(nop_lp_handler, test_db_config, test_feature_service, *_cache_tracker))
        , _semaphore(std::make_unique<reader_concurrency_semaphore>(reader_concurrency_semaphore::no_limits{}, "sstables::test_env"))
        , _db_config(test_db_config) {}

    future<> stop() {
        return _mgr->close().finally([this] {
            return _semaphore->stop();
        });
    }

    shared_sstable make_sstable(schema_ptr schema, sstring dir, unsigned long generation,
            sstable::version_types v = sstables::get_highest_sstable_version(), sstable::format_types f = sstable::format_types::big,
            size_t buffer_size = default_sstable_buffer_size, gc_clock::time_point now = gc_clock::now()) {
        return _mgr->make_sstable(std::move(schema), dir, generation, v, f, now, default_io_error_handler_gen(), buffer_size);
    }

    struct sst_not_found : public std::runtime_error {
        sst_not_found(const sstring& dir, unsigned long generation)
            : std::runtime_error(format("no versions of sstable generation {} found in {}", generation, dir))
        {}
    };

    future<shared_sstable> reusable_sst(schema_ptr schema, sstring dir, unsigned long generation,
            sstable::version_types version, sstable::format_types f = sstable::format_types::big) {
        auto sst = make_sstable(std::move(schema), dir, generation, version, f);
        return sst->load().then([sst = std::move(sst)] {
            return make_ready_future<shared_sstable>(std::move(sst));
        });
    }

    // looks up the sstable in the given dir
    future<shared_sstable> reusable_sst(schema_ptr schema, sstring dir, unsigned long generation);

    test_env_sstables_manager& manager() { return *_mgr; }
    reader_concurrency_semaphore& semaphore() { return *_semaphore; }
    const db::config& db_config() const { return _db_config; }
    reader_permit make_reader_permit(const schema* const s, const char* n, db::timeout_clock::time_point timeout) {
        return _semaphore->make_tracking_only_permit(s, n, timeout);
    }
    reader_permit make_reader_permit(db::timeout_clock::time_point timeout = db::no_timeout) {
        return _semaphore->make_tracking_only_permit(nullptr, "test", timeout);
    }

    future<> working_sst(schema_ptr schema, sstring dir, unsigned long generation) {
        return reusable_sst(std::move(schema), dir, generation).then([] (auto ptr) { return make_ready_future<>(); });
    }

    template <typename Func>
    static inline auto do_with(Func&& func) {
        return seastar::do_with(test_env(), [func = std::move(func)] (test_env& env) mutable {
            return futurize_invoke(func, env).finally([&env] {
                return env.stop();
            });
        });
    }

    template <typename T, typename Func>
    static inline auto do_with(T&& rval, Func&& func) {
        return seastar::do_with(test_env(), std::forward<T>(rval), [func = std::move(func)] (test_env& env, T& val) mutable {
            return futurize_invoke(func, env, val).finally([&env] {
                return env.stop();
            });
        });
    }

    static inline future<> do_with_async(noncopyable_function<void (test_env&)> func) {
        return seastar::async([func = std::move(func)] {
            test_env env;
            auto close_env = defer([&] { env.stop().get(); });
            func(env);
        });
    }

    static inline future<> do_with_sharded_async(noncopyable_function<void (sharded<test_env>&)> func) {
        return seastar::async([func = std::move(func)] {
            sharded<test_env> env;
            env.start().get();
            auto stop = defer([&] { env.stop().get(); });
            func(env);
        });
    }

    template <typename T>
    static future<T> do_with_async_returning(noncopyable_function<T (test_env&)> func) {
        return seastar::async([func = std::move(func)] {
            test_env env;
            auto stop = defer([&] { env.stop().get(); });
            return func(env);
        });
    }
};

}   // namespace sstables
