/*
 * Copyright (C) 2019-present ScyllaDB
 *
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <cmath>

#include "compaction_backlog_manager.hh"
#include "incremental_compaction_strategy.hh"

namespace compaction {

// The only difference to size tiered backlog tracker is that it will calculate
// backlog contribution using total bytes of each sstable run instead of total
// bytes of an individual sstable object.
//
// The runs are taken directly from the sstable set of the compaction group the
// backlog is computed for, rather than being maintained by the tracker on every
// sstable replacement.
class incremental_backlog_tracker final : public compaction_backlog_tracker::impl {
    incremental_compaction_strategy_options _options;

    // Cached backlog contribution fields, recalculated lazily when _backlog_dirty is set.
    // Marked mutable because they are caches updated on the first backlog() or rates()
    // call after a change.
    mutable bool _backlog_dirty = true;
    mutable int64_t _total_bytes = 0;
    mutable int64_t _total_backlog_bytes = 0;
    mutable double _sstables_backlog_contribution = 0.0f;
    mutable std::unordered_set<sstables::run_id> _sstable_runs_contributing_backlog;
    // The shape of the tree, for the compaction rates: the runs eligible for compaction
    // and the tiers they fall into, logged with the rates.
    mutable size_t _runs = 0;
    mutable size_t _tiers = 0;
    mutable size_t _max_runs_in_tier = 0;
    // The tiers with enough runs to compact, the runs their next compactions read, and
    // the rate at which those runs' data was ingested: what the target is made of.
    mutable size_t _eligible_tiers = 0;
    mutable size_t _eligible_runs = 0;
    mutable utils::rate<utils::units::bytes> _pending_rate_from_sstables;

    // URGENCY_FACTOR: the runs the eligible tiers hold over the runs they hold at the
    // threshold, _eligible_runs / (_eligible_tiers * threshold), times the read discount.
    // 1 when every eligible tier has just reached the threshold and the reads pay for it,
    // more the further past the threshold the tiers are, and 1 with no tier eligible,
    // when the target is 0 anyway.
    double urgency_factor(double threshold, double read_discount) const;
    // The sstables a point read touched, on average, over the threshold, between
    // 1 / threshold and 1: how far the reads pay for the tiers past the threshold. A read
    // touching threshold sstables, or more, pays for a tier's worth of runs that should
    // have been merged, and the target stands; a read touching fewer, because the runs
    // hold no version of its key, as under unique keys, pays less, and the target is
    // discounted in proportion, to no less than 1 / threshold, where a tier settles at
    // threshold^2 runs. No reads at all, as in a bulk load, are reads touching nothing.
    static double read_discount(double threshold, double sstables_per_read);

    // Shortest period the data of a tier is considered ingested over. It keeps a tier
    // whose data was ingested in a burst, or whose span is unknown, from demanding to
    // be compacted at an arbitrary rate.
    static constexpr std::chrono::minutes min_ingestion_span{10};

    struct inflight_component {
        int64_t total_bytes = 0;
        double contribution = 0;
    };

    inflight_component compacted_backlog(const compaction_backlog_tracker::ongoing_compactions& ongoing_compactions) const;

    struct backlog_calculation_result {
        int64_t total_bytes;
        int64_t total_backlog_bytes;
        float sstables_backlog_contribution;
        std::unordered_set<sstables::run_id> sstable_runs_contributing_backlog;
        size_t runs;
        size_t tiers;
        size_t max_runs_in_tier;
        size_t eligible_tiers;
        size_t eligible_runs;
        utils::rate<utils::units::bytes> pending_rate_from_sstables;
    };

    // Recalculates the cached fields if the sstables changed since.
    void refresh(const compaction_backlog_source& src) const;

public:
    static double log4(double x) {
        static const double inv_log_4 = 1.0f / std::log(4);
        return log(x) * inv_log_4;
    }

    static backlog_calculation_result calculate_sstables_backlog_contribution(const compaction_backlog_source& src, const incremental_compaction_strategy_options& options);

    incremental_backlog_tracker(incremental_compaction_strategy_options options);

    virtual double backlog(const compaction_backlog_source& src, const compaction_backlog_tracker::ongoing_writes& ow, const compaction_backlog_tracker::ongoing_compactions& oc) const override;
    // The target is ingestion_rate * tiers * URGENCY_FACTOR, with tiers the ones holding
    // at least min_threshold runs and URGENCY_FACTOR their runs over tiers * min_threshold
    // times the read discount, i.e. ingestion_rate * runs / min_threshold * read_discount:
    // the rate at which the compaction of each such tier ends by the time the tier has
    // min_threshold new runs, i.e. the next one is due, insofar as the reads pay for the
    // runs past the threshold. Every eligible tier asks for at least the ingestion rate,
    // whatever the reads, and for more in proportion to the runs it holds past the
    // threshold. A tier below the threshold has nothing to compact and asks for nothing.
    // The tiers stand in for the write amplification: each is one more rewrite of what
    // is ingested.
    //
    // ingestion_rate is the group's flushed bytes. When nothing was flushed within the
    // rate window, e.g. after a restart, the rate at which the data of the pending tiers
    // was ingested, from the ingestion metadata of their sstables, stands in.
    virtual compaction_rates rates(const compaction_backlog_source& src) const override;

    // The replaced sstables are already reflected in the group's sstable set, so this
    // only invalidates the cached backlog contribution.
    virtual void replace_sstables(const std::vector<sstables::shared_sstable>& old_ssts, const std::vector<sstables::shared_sstable>& new_ssts) override;
};

}
