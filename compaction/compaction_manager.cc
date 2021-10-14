/*
 * Copyright (C) 2015-present ScyllaDB
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

#include "compaction_manager.hh"
#include "compaction_strategy.hh"
#include "compaction_backlog_manager.hh"
#include "sstables/sstables.hh"
#include "sstables/sstables_manager.hh"
#include "database.hh"
#include <seastar/core/metrics.hh>
#include <seastar/core/coroutine.hh>
#include "sstables/exceptions.hh"
#include "locator/abstract_replication_strategy.hh"
#include "utils/fb_utilities.hh"
#include "utils/UUID_gen.hh"
#include <cmath>

static logging::logger cmlog("compaction_manager");
using namespace std::chrono_literals;

class compacting_sstable_registration {
    compaction_manager* _cm;
    std::vector<sstables::shared_sstable> _compacting;
public:
    compacting_sstable_registration(compaction_manager* cm, std::vector<sstables::shared_sstable> compacting)
        : _cm(cm)
        , _compacting(std::move(compacting))
    {
        _cm->register_compacting_sstables(_compacting);
    }

    compacting_sstable_registration& operator=(const compacting_sstable_registration&) = delete;
    compacting_sstable_registration(const compacting_sstable_registration&) = delete;

    compacting_sstable_registration& operator=(compacting_sstable_registration&& other) noexcept {
        if (this != &other) {
            this->~compacting_sstable_registration();
            new (this) compacting_sstable_registration(std::move(other));
        }
        return *this;
    }

    compacting_sstable_registration(compacting_sstable_registration&& other) noexcept
        : _cm(other._cm)
        , _compacting(std::move(other._compacting))
    {
        other._cm = nullptr;
    }

    ~compacting_sstable_registration() {
        if (_cm) {
            _cm->deregister_compacting_sstables(_compacting);
        }
    }

    // Explicitly release compacting sstables
    void release_compacting(const std::vector<sstables::shared_sstable>& sstables) {
        _cm->deregister_compacting_sstables(sstables);
        for (auto& sst : sstables) {
            _compacting.erase(boost::remove(_compacting, sst), _compacting.end());
        }
    }
};

lw_shared_ptr<sstables::compaction_data> compaction_manager::create_compaction_data(column_family& cf, sstables::compaction_type type) {
    auto info = make_lw_shared<sstables::compaction_data>();
    info->compaction_uuid = utils::UUID_gen::get_time_UUID();
    return info;
}

compaction_weight_registration::compaction_weight_registration(compaction_manager* cm, int weight)
    : _cm(cm)
    , _weight(weight)
{
    _cm->register_weight(_weight);
}

compaction_weight_registration& compaction_weight_registration::operator=(compaction_weight_registration&& other) noexcept {
    if (this != &other) {
        this->~compaction_weight_registration();
        new (this) compaction_weight_registration(std::move(other));
    }
    return *this;
}

compaction_weight_registration::compaction_weight_registration(compaction_weight_registration&& other) noexcept
    : _cm(other._cm)
    , _weight(other._weight)
{
    other._cm = nullptr;
    other._weight = 0;
}

compaction_weight_registration::~compaction_weight_registration() {
    if (_cm) {
        _cm->deregister_weight(_weight);
    }
}

void compaction_weight_registration::deregister() {
    _cm->deregister_weight(_weight);
    _cm = nullptr;
}

int compaction_weight_registration::weight() const {
    return _weight;
}

static inline uint64_t get_total_size(const std::vector<sstables::shared_sstable>& sstables) {
    uint64_t total_size = 0;
    for (auto& sst : sstables) {
        total_size += sst->data_size();
    }
    return total_size;
}

// Calculate weight of compaction job.
static inline int calculate_weight(uint64_t total_size) {
    // At the moment, '4' is being used as log base for determining the weight
    // of a compaction job. With base of 4, what happens is that when you have
    // a 40-second compaction in progress, and a tiny 10-second compaction
    // comes along, you do them in parallel.
    // TODO: Find a possibly better log base through experimentation.
    static constexpr int WEIGHT_LOG_BASE = 4;
    // Fixed tax is added to size before taking the log, to make sure all jobs
    // smaller than the tax (i.e. 1MB) will be serialized.
    static constexpr int fixed_size_tax = 1024*1024;

    // computes the logarithm (base WEIGHT_LOG_BASE) of total_size.
    return int(std::log(total_size + fixed_size_tax) / std::log(WEIGHT_LOG_BASE));
}

static inline int calculate_weight(const std::vector<sstables::shared_sstable>& sstables) {
    if (sstables.empty()) {
        return 0;
    }
    return calculate_weight(get_total_size(sstables));
}

bool compaction_manager::can_register_weight(column_family* cf, int weight) const {
    // Only one weight is allowed if parallel compaction is disabled.
    if (!cf->get_compaction_strategy().parallel_compaction() && has_table_ongoing_compaction(cf)) {
        return false;
    }
    // TODO: Maybe allow only *smaller* compactions to start? That can be done
    // by returning true only if weight is not in the set and is lower than any
    // entry in the set.
    if (_weight_tracker.contains(weight)) {
        // If reached this point, it means that there is an ongoing compaction
        // with the weight of the compaction job.
        return false;
    }
    return true;
}

void compaction_manager::register_weight(int weight) {
    _weight_tracker.insert(weight);
}

void compaction_manager::deregister_weight(int weight) {
    _weight_tracker.erase(weight);
    reevaluate_postponed_compactions();
}

std::vector<sstables::shared_sstable> compaction_manager::get_candidates(const column_family& cf) {
    std::vector<sstables::shared_sstable> candidates;
    candidates.reserve(cf.sstables_count());
    // prevents sstables that belongs to a partial run being generated by ongoing compaction from being
    // selected for compaction, which could potentially result in wrong behavior.
    auto partial_run_identifiers = boost::copy_range<std::unordered_set<utils::UUID>>(_tasks
            | boost::adaptors::transformed(std::mem_fn(&task::output_run_identifier)));
    auto compacting_run_identifiers = boost::copy_range<std::unordered_set<utils::UUID>>(_compacting_sstables
            | boost::adaptors::transformed(std::mem_fn(&sstables::sstable::run_identifier)));
    auto& cs = cf.get_compaction_strategy();

    // Filter out sstables that are being compacted.
    for (auto& run : cf.in_strategy_sstable_runs()) {
        auto run_id = run.run_id();

        if (compacting_run_identifiers.contains(run_id)) {
            continue;
        }
        if (!cs.can_compact_partial_runs() && partial_run_identifiers.contains(run_id)) {
            continue;
        }
        candidates.reserve(candidates.size() + run.all().size());
        candidates.insert(candidates.end(), std::make_move_iterator(run.all().begin()), std::make_move_iterator(run.all().end()));
    }
    return candidates;
}

void compaction_manager::register_compacting_sstables(const std::vector<sstables::shared_sstable>& sstables) {
    std::unordered_set<sstables::shared_sstable> sstables_to_merge;
    sstables_to_merge.reserve(sstables.size());
    for (auto& sst : sstables) {
        sstables_to_merge.insert(sst);
    }

    // make all required allocations in advance to merge
    // so it should not throw
    _compacting_sstables.reserve(_compacting_sstables.size() + sstables.size());
    try {
        _compacting_sstables.merge(sstables_to_merge);
    } catch (...) {
        cmlog.error("Unexpected error when registering compacting SSTables: {}. Ignored...", std::current_exception());
    }
}

void compaction_manager::deregister_compacting_sstables(const std::vector<sstables::shared_sstable>& sstables) {
    // Remove compacted sstables from the set of compacting sstables.
    for (auto& sst : sstables) {
        _compacting_sstables.erase(sst);
    }
}

class user_initiated_backlog_tracker final : public compaction_backlog_tracker::impl {
public:
    explicit user_initiated_backlog_tracker(float added_backlog, size_t available_memory) : _added_backlog(added_backlog), _available_memory(available_memory) {}
private:
    float _added_backlog;
    size_t _available_memory;
    virtual double backlog(const compaction_backlog_tracker::ongoing_writes& ow, const compaction_backlog_tracker::ongoing_compactions& oc) const override {
        return _added_backlog * _available_memory;
    }
    virtual void add_sstable(sstables::shared_sstable sst)  override { }
    virtual void remove_sstable(sstables::shared_sstable sst)  override { }
};

future<> compaction_manager::submit_major_compaction(column_family* cf) {
    if (_state != state::enabled) {
        return make_ready_future<>();
    }
    auto task = make_lw_shared<compaction_manager::task>(cf, sstables::compaction_type::Compaction);
    _tasks.push_back(task);

    // first take major compaction semaphore, then exclusely take compaction lock for column family.
    // it cannot be the other way around, or minor compaction for this column family would be
    // prevented while an ongoing major compaction doesn't release the semaphore.
    task->compaction_done = with_semaphore(_major_compaction_sem, 1, [this, task, cf] {
        return with_lock(_compaction_locks[cf].for_write(), [this, task, cf] {
            _stats.active_tasks++;
            if (!can_proceed(task)) {
                return make_ready_future<>();
            }

            // candidates are sstables that aren't being operated on by other compaction types.
            // those are eligible for major compaction.
            sstables::compaction_strategy cs = cf->get_compaction_strategy();
            sstables::compaction_descriptor descriptor = cs.get_major_compaction_job(*cf, get_candidates(*cf));
            auto compacting = make_lw_shared<compacting_sstable_registration>(this, descriptor.sstables);
            descriptor.release_exhausted = [compacting] (const std::vector<sstables::shared_sstable>& exhausted_sstables) {
                compacting->release_compacting(exhausted_sstables);
            };
            task->setup_new_compaction();
            task->output_run_identifier = descriptor.run_identifier;

            cmlog.info0("User initiated compaction started on behalf of {}.{}", cf->schema()->ks_name(), cf->schema()->cf_name());
            compaction_backlog_tracker user_initiated(std::make_unique<user_initiated_backlog_tracker>(_compaction_controller.backlog_of_shares(200), _available_memory));
            return do_with(std::move(user_initiated), [this, cf, descriptor = std::move(descriptor), task] (compaction_backlog_tracker& bt) mutable {
                register_backlog_tracker(bt);
                return with_scheduling_group(_compaction_controller.sg(), [this, cf, descriptor = std::move(descriptor), task] () mutable {
                    return cf->compact_sstables(std::move(descriptor), *task->compaction_data);
                });
            }).then([compacting = std::move(compacting)] {});
        });
    }).then_wrapped([this, task] (future<> f) {
        _stats.active_tasks--;
        _tasks.remove(task);
        try {
            f.get();
            _stats.completed_tasks++;
        } catch (sstables::compaction_stopped_exception& e) {
            cmlog.info("major compaction stopped, reason: {}", e.what());
        } catch (...) {
            cmlog.error("major compaction failed, reason: {}", std::current_exception());
            _stats.errors++;
        }
    });
    return task->compaction_done.get_future().then([task] {});
}

future<> compaction_manager::run_custom_job(column_family* cf, sstables::compaction_type type, noncopyable_function<future<>(sstables::compaction_data&)> job) {
    if (_state != state::enabled) {
        return make_ready_future<>();
    }

    auto task = make_lw_shared<compaction_manager::task>(cf, type);
    _tasks.push_back(task);

    auto job_ptr = std::make_unique<noncopyable_function<future<>(sstables::compaction_data&)>>(std::move(job));

    task->compaction_done = with_semaphore(_custom_job_sem, 1, [this, task, cf, &job = *job_ptr] () mutable {
        // take read lock for cf, so major compaction and resharding can't proceed in parallel.
        return with_lock(_compaction_locks[cf].for_read(), [this, task, cf, &job] () mutable {
            _stats.active_tasks++;
            if (!can_proceed(task)) {
                return make_ready_future<>();
            }
            task->setup_new_compaction();

            // NOTE:
            // no need to register shared sstables because they're excluded from non-resharding
            // compaction and some of them may not even belong to current shard.
            return job(*task->compaction_data);
        });
    }).then_wrapped([this, task, job_ptr = std::move(job_ptr)] (future<> f) {
        _stats.active_tasks--;
        _tasks.remove(task);
        try {
            f.get();
        } catch (sstables::compaction_stopped_exception& e) {
            cmlog.info("{} was abruptly stopped, reason: {}", task->type, e.what());
            throw;
        } catch (...) {
            cmlog.error("{} failed: {}", task->type, std::current_exception());
            throw;
        }
    });
    return task->compaction_done.get_future().then([task] {});
}

void compaction_manager::task::setup_new_compaction() {
    compaction_data = create_compaction_data(*compacting_cf, type);
    compaction_running = true;
}

void compaction_manager::task::finish_compaction() {
    compaction_running = false;
}

future<> compaction_manager::task_stop(lw_shared_ptr<compaction_manager::task> task, sstring reason) {
    task->stopping = true;
    task->compaction_data->stop(reason);
    auto f = task->compaction_done.get_future();
    return f.then([task] {
        task->stopping = false;
        return make_ready_future<>();
    });
}

compaction_manager::compaction_manager(compaction_scheduling_group csg, maintenance_scheduling_group msg, size_t available_memory, abort_source& as)
    : _compaction_controller(csg.cpu, csg.io, 250ms, [this, available_memory] () -> float {
        _last_backlog = backlog();
        auto b = _last_backlog / available_memory;
        // This means we are using an unimplemented strategy
        if (compaction_controller::backlog_disabled(b)) {
            // returning the normalization factor means that we'll return the maximum
            // output in the _control_points. We can get rid of this when we implement
            // all strategies.
            return compaction_controller::normalization_factor;
        }
        return b;
    })
    , _backlog_manager(_compaction_controller)
    , _maintenance_sg(msg)
    , _available_memory(available_memory)
    , _early_abort_subscription(as.subscribe([this] () noexcept {
        do_stop();
    }))
{
    register_metrics();
}

compaction_manager::compaction_manager(compaction_scheduling_group csg, maintenance_scheduling_group msg, size_t available_memory, uint64_t shares, abort_source& as)
    : _compaction_controller(csg.cpu, csg.io, shares)
    , _backlog_manager(_compaction_controller)
    , _maintenance_sg(msg)
    , _available_memory(available_memory)
    , _early_abort_subscription(as.subscribe([this] () noexcept {
        do_stop();
    }))
{
    register_metrics();
}

compaction_manager::compaction_manager()
    : _compaction_controller(seastar::default_scheduling_group(), default_priority_class(), 1)
    , _backlog_manager(_compaction_controller)
    , _maintenance_sg(maintenance_scheduling_group{default_scheduling_group(), default_priority_class()})
    , _available_memory(1)
{
    // No metric registration because this constructor is supposed to be used only by the testing
    // infrastructure.
}

compaction_manager::~compaction_manager() {
    // Assert that compaction manager was explicitly stopped, if started.
    // Otherwise, fiber(s) will be alive after the object is stopped.
    assert(_state == state::none || _state == state::stopped);
}

void compaction_manager::register_metrics() {
    namespace sm = seastar::metrics;

    _metrics.add_group("compaction_manager", {
        sm::make_gauge("compactions", [this] { return _stats.active_tasks; },
                       sm::description("Holds the number of currently active compactions.")),
        sm::make_gauge("pending_compactions", [this] { return _stats.pending_tasks; },
                       sm::description("Holds the number of compaction tasks waiting for an opportunity to run.")),
        sm::make_gauge("backlog", [this] { return _last_backlog; },
                       sm::description("Holds the sum of compaction backlog for all tables in the system.")),
    });
}

void compaction_manager::enable() {
    assert(_state == state::none || _state == state::disabled);
    _state = state::enabled;
    _compaction_submission_timer.arm(periodic_compaction_submission_interval());
    postponed_compactions_reevaluation();
}

void compaction_manager::disable() {
    assert(_state == state::none || _state == state::enabled);
    _state = state::disabled;
    _compaction_submission_timer.cancel();
}

std::function<void()> compaction_manager::compaction_submission_callback() {
    return [this] () mutable {
        for (auto& e: _compaction_locks) {
            submit(e.first);
        }
    };
}

void compaction_manager::postponed_compactions_reevaluation() {
    _waiting_reevalution = repeat([this] {
        return _postponed_reevaluation.wait().then([this] {
            if (_state != state::enabled) {
                _postponed.clear();
                return stop_iteration::yes;
            }
            auto postponed = std::move(_postponed);
            try {
                for (auto& cf : postponed) {
                    submit(cf);
                }
            } catch (...) {
                _postponed = std::move(postponed);
            }
            return stop_iteration::no;
        });
    });
}

void compaction_manager::reevaluate_postponed_compactions() {
    _postponed_reevaluation.signal();
}

void compaction_manager::postpone_compaction_for_column_family(column_family* cf) {
    _postponed.insert(cf);
}

future<> compaction_manager::stop_ongoing_compactions(sstring reason) {
    cmlog.info("Stopping {} ongoing compactions due to {}", get_compactions().size(), reason);

    // Wait for each task handler to stop. Copy list because task remove itself
    // from the list when done.
    auto tasks = _tasks;
    return do_with(std::move(tasks), [this, reason] (std::list<lw_shared_ptr<task>>& tasks) {
        return parallel_for_each(tasks, [this, reason] (auto& task) {
            return this->task_stop(task, reason).then_wrapped([](future <> f) {
                try {
                    f.get();
                } catch (sstables::compaction_stopped_exception& e) {
                    // swallow stop exception if a given procedure decides to propagate it to the caller,
                    // as it happens with reshard and reshape.
                } catch (...) {
                    throw;
                }
            });
        });
    });
}

future<> compaction_manager::drain() {
    _state = state::disabled;
    return stop_ongoing_compactions("drain");
}

future<> compaction_manager::stop() {
    // never started
    if (_state == state::none) {
        return make_ready_future<>();
    } else {
        do_stop();
        return std::move(*_stop_future);
    }
}

void compaction_manager::really_do_stop() {
    if (_state == state::none || _state == state::stopped) {
        return;
    }

    _state = state::stopped;
    cmlog.info("Asked to stop");
    // Reset the metrics registry
    _metrics.clear();
    _stop_future.emplace(stop_ongoing_compactions("shutdown").then([this] () mutable {
        reevaluate_postponed_compactions();
        return std::move(_waiting_reevalution);
    }).then([this] {
        _weight_tracker.clear();
        _compaction_submission_timer.cancel();
        cmlog.info("Stopped");
        return _compaction_controller.shutdown();
    }));
}

void compaction_manager::do_stop() noexcept {
    try {
        really_do_stop();
    } catch (...) {
        try {
            cmlog.error("Failed to stop the manager: {}", std::current_exception());
        } catch (...) {
            // Nothing else we can do.
        }
    }
}

inline bool compaction_manager::can_proceed(const lw_shared_ptr<task>& task) {
    return (_state == state::enabled) && !task->stopping;
}

inline future<> compaction_manager::put_task_to_sleep(lw_shared_ptr<task>& task) {
    cmlog.info("compaction task handler sleeping for {} seconds",
        std::chrono::duration_cast<std::chrono::seconds>(task->compaction_retry.sleep_time()).count());
    return task->compaction_retry.retry();
}

inline bool compaction_manager::maybe_stop_on_error(future<> f, stop_iteration will_stop) {
    bool retry = false;

    try {
        f.get();
    } catch (sstables::compaction_stopped_exception& e) {
        cmlog.info("compaction info: {}: stopping", e.what());
    } catch (sstables::compaction_aborted_exception& e) {
        cmlog.error("compaction info: {}: stopping", e.what());
        _stats.errors++;
    } catch (storage_io_error& e) {
        _stats.errors++;
        cmlog.error("compaction failed due to storage io error: {}: stopping", e.what());
        do_stop();
    } catch (...) {
        _stats.errors++;
        retry = (will_stop == stop_iteration::no);
        cmlog.error("compaction failed: {}: {}", std::current_exception(), retry ? "retrying" : "stopping");
    }
    return retry;
}

void compaction_manager::submit(column_family* cf) {
    if (cf->is_auto_compaction_disabled_by_user()) {
        return;
    }

    auto task = make_lw_shared<compaction_manager::task>(cf, sstables::compaction_type::Compaction);
    _tasks.push_back(task);
    _stats.pending_tasks++;

    task->compaction_done = repeat([this, task, cf] () mutable {
        if (!can_proceed(task)) {
            _stats.pending_tasks--;
            return make_ready_future<stop_iteration>(stop_iteration::yes);
        }
        return with_lock(_compaction_locks[cf].for_read(), [this, task] () mutable {
          return with_scheduling_group(_compaction_controller.sg(), [this, task = std::move(task)] () mutable {
            column_family& cf = *task->compacting_cf;
            sstables::compaction_strategy cs = cf.get_compaction_strategy();
            sstables::compaction_descriptor descriptor = cs.get_sstables_for_compaction(cf, get_candidates(cf));
            int weight = calculate_weight(descriptor.sstables);

            if (descriptor.sstables.empty() || !can_proceed(task) || cf.is_auto_compaction_disabled_by_user()) {
                _stats.pending_tasks--;
                return make_ready_future<stop_iteration>(stop_iteration::yes);
            }
            if (!can_register_weight(&cf, weight)) {
                _stats.pending_tasks--;
                cmlog.debug("Refused compaction job ({} sstable(s)) of weight {} for {}.{}, postponing it...",
                    descriptor.sstables.size(), weight, cf.schema()->ks_name(), cf.schema()->cf_name());
                postpone_compaction_for_column_family(&cf);
                return make_ready_future<stop_iteration>(stop_iteration::yes);
            }
            auto compacting = make_lw_shared<compacting_sstable_registration>(this, descriptor.sstables);
            auto weight_r = compaction_weight_registration(this, weight);
            descriptor.release_exhausted = [compacting] (const std::vector<sstables::shared_sstable>& exhausted_sstables) {
                compacting->release_compacting(exhausted_sstables);
            };
            cmlog.debug("Accepted compaction job ({} sstable(s)) of weight {} for {}.{}",
                descriptor.sstables.size(), weight, cf.schema()->ks_name(), cf.schema()->cf_name());

            _stats.pending_tasks--;
            _stats.active_tasks++;
            task->setup_new_compaction();
            task->output_run_identifier = descriptor.run_identifier;
            return cf.compact_sstables(std::move(descriptor), *task->compaction_data).then_wrapped([this, task, compacting = std::move(compacting), weight_r = std::move(weight_r)] (future<> f) mutable {
                _stats.active_tasks--;
                task->finish_compaction();

                if (!can_proceed(task)) {
                    maybe_stop_on_error(std::move(f), stop_iteration::yes);
                    return make_ready_future<stop_iteration>(stop_iteration::yes);
                }
                if (maybe_stop_on_error(std::move(f))) {
                    _stats.pending_tasks++;
                    return put_task_to_sleep(task).then([] {
                        return make_ready_future<stop_iteration>(stop_iteration::no);
                    });
                }
                _stats.pending_tasks++;
                _stats.completed_tasks++;
                task->compaction_retry.reset();
                reevaluate_postponed_compactions();
                return make_ready_future<stop_iteration>(stop_iteration::no);
            });
          });
        });
    }).finally([this, task] {
        _tasks.remove(task);
    });
}

void compaction_manager::submit_offstrategy(column_family* cf) {
    auto task = make_lw_shared<compaction_manager::task>(cf, sstables::compaction_type::Reshape);
    _tasks.push_back(task);
    _stats.pending_tasks++;

    task->compaction_done = repeat([this, task, cf] () mutable {
        if (!can_proceed(task)) {
            _stats.pending_tasks--;
            return make_ready_future<stop_iteration>(stop_iteration::yes);
        }
        return with_semaphore(_custom_job_sem, 1, [this, task, cf] () mutable {
            return with_lock(_compaction_locks[cf].for_read(), [this, task, cf] () mutable {
                _stats.pending_tasks--;
                if (!can_proceed(task)) {
                    return make_ready_future<stop_iteration>(stop_iteration::yes);
                }
                _stats.active_tasks++;
                task->setup_new_compaction();

                return cf->run_offstrategy_compaction(*task->compaction_data).then_wrapped([this, task] (future<> f) mutable {
                    _stats.active_tasks--;
                    task->finish_compaction();
                    try {
                        f.get();
                        _stats.completed_tasks++;
                    } catch (sstables::compaction_stopped_exception& e) {
                        cmlog.info("off-strategy compaction: {}", e.what());
                    } catch (sstables::compaction_aborted_exception& e) {
                        _stats.errors++;
                        cmlog.error("off-strategy compaction: {}", e.what());
                    } catch (...) {
                        _stats.errors++;
                        _stats.pending_tasks++;
                        cmlog.error("off-strategy compaction failed due to {}, retrying...", std::current_exception());
                        return put_task_to_sleep(task).then([] {
                            return make_ready_future<stop_iteration>(stop_iteration::no);
                        });
                    }
                    _tasks.remove(task);
                    return make_ready_future<stop_iteration>(stop_iteration::yes);
                });
            });
        });
    });
}

inline bool compaction_manager::check_for_cleanup(column_family* cf) {
    for (auto& task : _tasks) {
        if (task->compacting_cf == cf && task->type == sstables::compaction_type::Cleanup) {
            return true;
        }
    }
    return false;
}

future<> compaction_manager::rewrite_sstables(column_family* cf, sstables::compaction_type_options options, get_candidates_func get_func, can_purge_tombstones can_purge) {
    auto task = make_lw_shared<compaction_manager::task>(cf, options.type());
    _tasks.push_back(task);

    auto sstables = std::make_unique<std::vector<sstables::shared_sstable>>(get_func(*cf));
    // sort sstables by size in descending order, such that the smallest files will be rewritten first
    // (as sstable to be rewritten is popped off from the back of container), so rewrite will have higher
    // chance to succeed when the biggest files are reached.
    std::sort(sstables->begin(), sstables->end(), [](sstables::shared_sstable& a, sstables::shared_sstable& b) {
        return a->data_size() > b->data_size();
    });

    auto compacting = make_lw_shared<compacting_sstable_registration>(this, *sstables);
    auto sstables_ptr = sstables.get();
    _stats.pending_tasks += sstables->size();

    task->compaction_done = do_until([this, sstables_ptr, task] { return sstables_ptr->empty() || !can_proceed(task); },
             [this, task, options, sstables_ptr, compacting, can_purge] () mutable {
        auto sst = sstables_ptr->back();
        sstables_ptr->pop_back();

        return repeat([this, task, options, sst = std::move(sst), compacting, can_purge] () mutable {
            column_family& cf = *task->compacting_cf;
            auto sstable_level = sst->get_sstable_level();
            auto run_identifier = sst->run_identifier();
            auto sstable_set_snapshot = can_purge ? std::make_optional(cf.get_sstable_set()) : std::nullopt;
            auto descriptor = sstables::compaction_descriptor({ sst }, std::move(sstable_set_snapshot), _maintenance_sg.io,
                sstable_level, sstables::compaction_descriptor::default_max_sstable_bytes, run_identifier, options);

            // Releases reference to cleaned sstable such that respective used disk space can be freed.
            descriptor.release_exhausted = [compacting] (const std::vector<sstables::shared_sstable>& exhausted_sstables) {
                compacting->release_compacting(exhausted_sstables);
            };

            return with_semaphore(_rewrite_sstables_sem, 1, [this, task, &cf, descriptor = std::move(descriptor), compacting] () mutable {
              // Take write lock for cf to serialize cleanup/upgrade sstables/scrub with major compaction/reshape/reshard.
              return with_lock(_compaction_locks[&cf].for_write(), [this, task, &cf, descriptor = std::move(descriptor), compacting] () mutable {
                _stats.pending_tasks--;
                _stats.active_tasks++;
                task->setup_new_compaction();
                task->output_run_identifier = descriptor.run_identifier;
                compaction_backlog_tracker user_initiated(std::make_unique<user_initiated_backlog_tracker>(_compaction_controller.backlog_of_shares(200), _available_memory));
                return do_with(std::move(user_initiated), [this, &cf, descriptor = std::move(descriptor), task] (compaction_backlog_tracker& bt) mutable {
                    return with_scheduling_group(_maintenance_sg.cpu, [this, &cf, descriptor = std::move(descriptor), task]() mutable {
                        return cf.compact_sstables(std::move(descriptor), *task->compaction_data);
                    });
                });
              });
            }).then_wrapped([this, task, compacting] (future<> f) mutable {
                task->finish_compaction();
                _stats.active_tasks--;
                if (!can_proceed(task)) {
                    maybe_stop_on_error(std::move(f), stop_iteration::yes);
                    return make_ready_future<stop_iteration>(stop_iteration::yes);
                }
                if (maybe_stop_on_error(std::move(f))) {
                    _stats.pending_tasks++;
                    return put_task_to_sleep(task).then([] {
                        return make_ready_future<stop_iteration>(stop_iteration::no);
                    });
                }
                _stats.completed_tasks++;
                reevaluate_postponed_compactions();
                return make_ready_future<stop_iteration>(stop_iteration::yes);
            });
        });
    }).finally([this, task, sstables = std::move(sstables)] {
        _stats.pending_tasks -= sstables->size();
        _tasks.remove(task);
    });

    return task->compaction_done.get_future().then([task] {});
}

future<> compaction_manager::perform_sstable_scrub_validate_mode(column_family* cf) {
    // All sstables must be included, even the ones being compacted, such that everything in table is validated.
    auto all_sstables = boost::copy_range<std::vector<sstables::shared_sstable>>(*cf->get_sstables());
    return run_custom_job(cf, sstables::compaction_type::Scrub, [this, &cf = *cf, sstables = std::move(all_sstables)] (sstables::compaction_data& info) mutable -> future<> {
        class pending_tasks {
            compaction_manager::stats& _stats;
            size_t _n;
        public:
            pending_tasks(compaction_manager::stats& stats, size_t n) : _stats(stats), _n(n) { _stats.pending_tasks += _n; }
            ~pending_tasks() { _stats.pending_tasks -= _n; }
            void operator--(int) {
                --_stats.pending_tasks;
                --_n;
            }
        };
        pending_tasks pending(_stats, sstables.size());

        while (!sstables.empty()) {
            auto sst = sstables.back();
            sstables.pop_back();

            try {
                co_await with_scheduling_group(_maintenance_sg.cpu, [&] () {
                    auto desc = sstables::compaction_descriptor(
                            { sst },
                            {},
                            _maintenance_sg.io,
                            sst->get_sstable_level(),
                            sstables::compaction_descriptor::default_max_sstable_bytes,
                            sst->run_identifier(),
                            sstables::compaction_type_options::make_scrub(sstables::compaction_type_options::scrub::mode::validate));
                    return compact_sstables(std::move(desc), info, cf);
                });
            } catch (sstables::compaction_stopped_exception&) {
                throw; // let run_custom_job() handle this
            } catch (storage_io_error&) {
                throw; // let run_custom_job() handle this
            } catch (...) {
                // We are validating potentially corrupt sstables, errors are
                // expected, just continue with the other sstables when seeing
                // one.
                _stats.errors++;
                cmlog.error("Scrubbing in validate mode {} failed due to {}, continuing.", sst->get_filename(), std::current_exception());
            }

            pending--;
        }
    });
}

bool needs_cleanup(const sstables::shared_sstable& sst,
                   const dht::token_range_vector& sorted_owned_ranges,
                   schema_ptr s) {
    auto first = sst->get_first_partition_key();
    auto last = sst->get_last_partition_key();
    auto first_token = dht::get_token(*s, first);
    auto last_token = dht::get_token(*s, last);
    dht::token_range sst_token_range = dht::token_range::make(first_token, last_token);

    auto r = std::lower_bound(sorted_owned_ranges.begin(), sorted_owned_ranges.end(), first_token,
            [] (const range<dht::token>& a, const dht::token& b) {
        // check that range a is before token b.
        return a.after(b, dht::token_comparator());
    });

    // return true iff sst partition range isn't fully contained in any of the owned ranges.
    if (r != sorted_owned_ranges.end()) {
        if (r->contains(sst_token_range, dht::token_comparator())) {
            return false;
        }
    }
    return true;
}

future<> compaction_manager::perform_cleanup(database& db, column_family* cf) {
    if (check_for_cleanup(cf)) {
        return make_exception_future<>(std::runtime_error(format("cleanup request failed: there is an ongoing cleanup on {}.{}",
            cf->schema()->ks_name(), cf->schema()->cf_name())));
    }
    return seastar::async([this, cf, &db] {
        auto schema = cf->schema();
        auto& rs = db.find_keyspace(schema->ks_name()).get_replication_strategy();
        auto sorted_owned_ranges = rs.get_ranges(utils::fb_utilities::get_broadcast_address(), utils::can_yield::yes);
        auto sstables = std::vector<sstables::shared_sstable>{};
        const auto candidates = get_candidates(*cf);
        std::copy_if(candidates.begin(), candidates.end(), std::back_inserter(sstables), [&sorted_owned_ranges, schema] (const sstables::shared_sstable& sst) {
            seastar::thread::maybe_yield();
            return sorted_owned_ranges.empty() || needs_cleanup(sst, sorted_owned_ranges, schema);
        });
        return sstables;
    }).then([this, cf, &db] (std::vector<sstables::shared_sstable> sstables) {
        return rewrite_sstables(cf, sstables::compaction_type_options::make_cleanup(db),
                [sstables = std::move(sstables)] (const table&) { return sstables; });
    });
}

// Submit a column family to be upgraded and wait for its termination.
future<> compaction_manager::perform_sstable_upgrade(database& db, column_family* cf, bool exclude_current_version) {
    using shared_sstables = std::vector<sstables::shared_sstable>;
    return do_with(shared_sstables{}, [this, &db, cf, exclude_current_version](shared_sstables& tables) {
        // since we might potentially have ongoing compactions, and we
        // must ensure that all sstables created before we run are included
        // in the re-write, we need to barrier out any previously running
        // compaction.
        return cf->run_with_compaction_disabled([this, cf, &tables, exclude_current_version] {
            auto last_version = cf->get_sstables_manager().get_highest_supported_format();

            for (auto& sst : get_candidates(*cf)) {
                // if we are a "normal" upgrade, we only care about
                // tables with older versions, but potentially
                // we are to actually rewrite everything. (-a)
                if (!exclude_current_version || sst->get_version() < last_version) {
                    tables.emplace_back(sst);
                }
            }
            return make_ready_future<>();
        }).then([this, &db, cf, &tables] {
            // doing a "cleanup" is about as compacting as we need
            // to be, provided we get to decide the tables to process,
            // and ignoring any existing operations.
            // Note that we potentially could be doing multiple
            // upgrades here in parallel, but that is really the users
            // problem.
            return rewrite_sstables(cf, sstables::compaction_type_options::make_upgrade(db), [&](auto&) mutable {
                return std::exchange(tables, {});
            });
        });
    });
}

// Submit a column family to be scrubbed and wait for its termination.
future<> compaction_manager::perform_sstable_scrub(column_family* cf, sstables::compaction_type_options::scrub::mode scrub_mode) {
    if (scrub_mode == sstables::compaction_type_options::scrub::mode::validate) {
        return perform_sstable_scrub_validate_mode(cf);
    }
    // since we might potentially have ongoing compactions, and we
    // must ensure that all sstables created before we run are scrubbed,
    // we need to barrier out any previously running compaction.
    return cf->run_with_compaction_disabled([this, cf, scrub_mode] {
        return rewrite_sstables(cf, sstables::compaction_type_options::make_scrub(scrub_mode), [this] (const table& cf) {
            return get_candidates(cf);
        }, can_purge_tombstones::no);
    });
}

future<> compaction_manager::remove(column_family* cf) {
    // We need to guarantee that a task being stopped will not retry to compact
    // a column family being removed.
    auto tasks_to_stop = make_lw_shared<std::vector<lw_shared_ptr<task>>>();
    for (auto& task : _tasks) {
        if (task->compacting_cf == cf) {
            tasks_to_stop->push_back(task);
            task->stopping = true;
        }
    }
    _postponed.erase(cf);

    // Wait for the termination of an ongoing compaction on cf, if any.
    return parallel_for_each(*tasks_to_stop, [this, cf] (auto& task) {
        return this->task_stop(task, "column family removal");
    }).then([this, cf, tasks_to_stop] {
#ifdef DEBUG
        assert(std::find_if(_tasks.begin(), _tasks.end(), [cf] (auto& task) { return task->compacting_cf == cf; }) == _tasks.end());
#endif
        _compaction_locks.erase(cf);
    });
}

const std::vector<sstables::compaction_info> compaction_manager::get_compactions() const {
    auto to_info = [] (const lw_shared_ptr<task>& t) {
        sstables::compaction_info ret;
        ret.compaction_uuid = t->compaction_data->compaction_uuid;
        ret.type = t->type;
        ret.ks_name = t->compacting_cf->schema()->ks_name();
        ret.cf_name = t->compacting_cf->schema()->cf_name();
        ret.total_partitions = t->compaction_data->total_partitions;
        ret.total_keys_written = t->compaction_data->total_keys_written;
        return ret;
    };
    using ret = std::vector<sstables::compaction_info>;
    return boost::copy_range<ret>(_tasks | boost::adaptors::filtered(std::mem_fn(&task::compaction_running)) | boost::adaptors::transformed(to_info));
}

void compaction_manager::stop_compaction(sstring type) {
    sstables::compaction_type target_type;
    try {
        target_type = sstables::to_compaction_type(type);
    } catch (...) {
        throw std::runtime_error(format("Compaction of type {} cannot be stopped by compaction manager: {}", type.c_str(), std::current_exception()));
    }
    // FIXME: switch to task_stop(), and wait for their termination, so API user can know when compactions actually stopped.
    for (auto& task : _tasks) {
        if (task->compaction_running && target_type == task->type) {
            task->compaction_data->stop("user request");
        }
    }
}

void compaction_manager::propagate_replacement(column_family* cf,
        const std::vector<sstables::shared_sstable>& removed, const std::vector<sstables::shared_sstable>& added) {
    for (auto& task : _tasks) {
        if (task->compacting_cf == cf && task->compaction_running) {
            task->compaction_data->pending_replacements.push_back({ removed, added });
        }
    }
}

double compaction_backlog_tracker::backlog() const {
    return _disabled ? compaction_controller::disable_backlog : _impl->backlog(_ongoing_writes, _ongoing_compactions);
}

void compaction_backlog_tracker::add_sstable(sstables::shared_sstable sst) {
    if (_disabled || !sstable_belongs_to_tracker(sst)) {
        return;
    }
    _ongoing_writes.erase(sst);
    try {
        _impl->add_sstable(std::move(sst));
    } catch (...) {
        cmlog.warn("Disabling backlog tracker due to exception {}", std::current_exception());
        disable();
    }
}

void compaction_backlog_tracker::remove_sstable(sstables::shared_sstable sst) {
    if (_disabled || !sstable_belongs_to_tracker(sst)) {
        return;
    }

    _ongoing_compactions.erase(sst);
    try {
        _impl->remove_sstable(std::move(sst));
    } catch (...) {
        cmlog.warn("Disabling backlog tracker due to exception {}", std::current_exception());
        disable();
    }
}

bool compaction_backlog_tracker::sstable_belongs_to_tracker(const sstables::shared_sstable& sst) {
    return !sst->requires_view_building();
}

void compaction_backlog_tracker::register_partially_written_sstable(sstables::shared_sstable sst, backlog_write_progress_manager& wp) {
    if (_disabled) {
        return;
    }
    try {
        _ongoing_writes.emplace(sst, &wp);
    } catch (...) {
        // We can potentially recover from adding ongoing compactions or writes when the process
        // ends. The backlog will just be temporarily wrong. If we are are suffering from something
        // more serious like memory exhaustion we will soon fail again in either add / remove and
        // then we'll disable the tracker. For now, try our best.
        cmlog.warn("backlog tracker couldn't register partially written SSTable to exception {}", std::current_exception());
    }
}

void compaction_backlog_tracker::register_compacting_sstable(sstables::shared_sstable sst, backlog_read_progress_manager& rp) {
    if (_disabled) {
        return;
    }

    try {
        _ongoing_compactions.emplace(sst, &rp);
    } catch (...) {
        cmlog.warn("backlog tracker couldn't register partially compacting SSTable to exception {}", std::current_exception());
    }
}

void compaction_backlog_tracker::transfer_ongoing_charges(compaction_backlog_tracker& new_bt, bool move_read_charges) {
    for (auto&& w : _ongoing_writes) {
        new_bt.register_partially_written_sstable(w.first, *w.second);
    }

    if (move_read_charges) {
        for (auto&& w : _ongoing_compactions) {
            new_bt.register_compacting_sstable(w.first, *w.second);
        }
    }
    _ongoing_writes = {};
    _ongoing_compactions = {};
}

void compaction_backlog_tracker::revert_charges(sstables::shared_sstable sst) {
    _ongoing_writes.erase(sst);
    _ongoing_compactions.erase(sst);
}

compaction_backlog_tracker::~compaction_backlog_tracker() {
    if (_manager) {
        _manager->remove_backlog_tracker(this);
    }
}

void compaction_backlog_manager::remove_backlog_tracker(compaction_backlog_tracker* tracker) {
    _backlog_trackers.erase(tracker);
}

double compaction_backlog_manager::backlog() const {
    try {
        double backlog = 0;

        for (auto& tracker: _backlog_trackers) {
            backlog += tracker->backlog();
        }
        if (compaction_controller::backlog_disabled(backlog)) {
            return compaction_controller::disable_backlog;
        } else {
            return backlog;
        }
    } catch (...) {
        return _compaction_controller->backlog_of_shares(1000);
    }
}

void compaction_backlog_manager::register_backlog_tracker(compaction_backlog_tracker& tracker) {
    tracker._manager = this;
    _backlog_trackers.insert(&tracker);
}

compaction_backlog_manager::~compaction_backlog_manager() {
    for (auto* tracker : _backlog_trackers) {
        tracker->_manager = nullptr;
    }
}
