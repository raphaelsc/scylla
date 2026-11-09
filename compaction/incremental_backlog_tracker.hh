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
    // The sstables a point read touches at most with no tier above the threshold, one
    // per run: the read amplification the target aims for, which the measured one is
    // held against to tell how urgent the pending compactions are.
    mutable size_t _ideal_runs = 0;

    // How far reads touching fewer sstables than the ideal lower the target, at most:
    // the pending tiers then settle at min_threshold / min_urgency runs instead of
    // min_threshold, and no reads, as in a bulk load, leave the target there.
    static constexpr double min_urgency = 0.25;

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
        size_t ideal_runs;
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
    // The target is ingestion_rate * pending_runs / min_threshold, summed over the tiers
    // with at least min_threshold runs: the rate at which the compaction of each such
    // tier ends by the time the tier has min_threshold new runs, i.e. the next one is
    // due. A tier below the threshold has nothing to compact and asks for nothing.
    //
    // That sum is scaled by the urgency, between min_urgency and 1: the sstables the
    // group's point reads touch over the sstables they would touch with no tier above
    // the threshold. Reads touching fewer are not paying for the surplus, so it is
    // worked off more slowly; no reads at all leave the target at min_urgency of the
    // sum. While a tier is pending the target never drops below the ingestion rate:
    // every flushed byte is compacted at least once.
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
