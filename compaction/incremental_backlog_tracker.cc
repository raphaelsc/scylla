/*
 * Copyright (C) 2019-present ScyllaDB
 *
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <algorithm>

#include "incremental_backlog_tracker.hh"
#include "compaction.hh"
#include "sstables/sstables.hh"
#include "sstables/sstable_set.hh"
#include "utils/log.hh"

namespace compaction {

incremental_backlog_tracker::inflight_component incremental_backlog_tracker::compacted_backlog(const compaction_backlog_tracker::ongoing_compactions& ongoing_compactions) const {
    inflight_component in;
    for (auto& crp : ongoing_compactions) {
        if (!_sstable_runs_contributing_backlog.contains(crp.first->run_identifier())) {
            continue;
        }
        // Ci is left untaxed on purpose: the fixed cost belongs to the sstable existing,
        // not to the compaction reading it, so it's only retired once the sstable leaves
        // the set. Taxing Ci would cancel the tax added to Si for every sstable being
        // compacted, and prorating it would only decay it earlier.
        auto compacted = crp.second->compacted();
        in.total_bytes += compacted;
        in.contribution += compacted * log4((crp.first->data_size()));
    }
    return in;
}

incremental_backlog_tracker::backlog_calculation_result
incremental_backlog_tracker::calculate_sstables_backlog_contribution(const compaction_backlog_source& src, const incremental_compaction_strategy_options& options) {
    size_t threshold = src.schema()->min_compaction_threshold();
    size_t max_threshold = src.schema()->max_compaction_threshold();
    int64_t total_bytes = 0;
    int64_t total_backlog_bytes = 0;
    float sstables_backlog_contribution = 0.0f;
    std::unordered_set<sstables::run_id> sstable_runs_contributing_backlog = {};

    // Only runs eligible for compaction are accounted for, e.g. the ones still waiting
    // for view building are left out.
    std::vector<sstables::frozen_sstable_run> all;
    for (auto& run : src.sstables_for_backlog()->all_sstable_runs()) {
        if (is_eligible_for_compaction(run)) {
            total_bytes += run->data_size();
            all.push_back(run);
        }
    }

    size_t tiers = 0;
    size_t max_runs_in_tier = 0;
    size_t eligible_tiers = 0;
    size_t eligible_runs = 0;
    utils::rate<utils::units::bytes> pending_rate_from_sstables;
    size_t ideal_runs = 0;
    if (!all.empty()) {
      auto buckets = incremental_compaction_strategy::get_buckets(all, options);
      tiers = buckets.size();
      for (auto& bucket : buckets) {
        max_runs_in_tier = std::max(max_runs_in_tier, bucket.size());
        // A point read touches at most one sstable per run, so with no tier above the
        // threshold it touches at most this many.
        ideal_runs += std::min(bucket.size(), threshold);
        if (!incremental_compaction_strategy::is_bucket_interesting(bucket, threshold)) {
            continue;
        }
        eligible_tiers++;
        // The runs the tier's next compaction reads, at most what one compaction takes.
        // Runs being compacted are still in the set and count: the tier has work until
        // the compaction ends.
        auto runs = std::min(bucket.size(), max_threshold);
        eligible_runs += runs;
        pending_rate_from_sstables = pending_rate_from_sstables
                + compaction::ingestion_rate_from_sstables(bucket, min_ingestion_span) * (double(runs) / threshold);
        for (const sstables::frozen_sstable_run& run_ptr : bucket) {
            auto& run = *run_ptr;
            auto data_size = run.data_size();
            if (data_size > 0) {
                // Si is taxed with the fixed cost of every sstable in the run, where it
                // weighs the work, but not inside the log. See sstable_backlog_fixed_cost.
                auto size = effective_backlog_size(data_size, run.all().size());
                total_backlog_bytes += size;
                sstables_backlog_contribution += size * log4(data_size);
                sstable_runs_contributing_backlog.insert((*run.all().begin())->run_identifier());
            }
        }
      }
    }
    return backlog_calculation_result{
        .total_bytes = total_bytes,
        .total_backlog_bytes = total_backlog_bytes,
        .sstables_backlog_contribution = sstables_backlog_contribution,
        .sstable_runs_contributing_backlog = std::move(sstable_runs_contributing_backlog),
        .runs = all.size(),
        .tiers = tiers,
        .max_runs_in_tier = max_runs_in_tier,
        .eligible_tiers = eligible_tiers,
        .eligible_runs = eligible_runs,
        .pending_rate_from_sstables = pending_rate_from_sstables,
        .ideal_runs = ideal_runs,
    };
}

void incremental_backlog_tracker::refresh(const compaction_backlog_source& src) const {
    if (!_backlog_dirty) {
        return;
    }
    auto result = calculate_sstables_backlog_contribution(src, _options);
    _total_bytes = result.total_bytes;
    _total_backlog_bytes = result.total_backlog_bytes;
    _sstables_backlog_contribution = result.sstables_backlog_contribution;
    _sstable_runs_contributing_backlog = std::move(result.sstable_runs_contributing_backlog);
    _runs = result.runs;
    _tiers = result.tiers;
    _max_runs_in_tier = result.max_runs_in_tier;
    _eligible_tiers = result.eligible_tiers;
    _eligible_runs = result.eligible_runs;
    _pending_rate_from_sstables = result.pending_rate_from_sstables;
    _ideal_runs = result.ideal_runs;
    _backlog_dirty = false;
}

compaction_rates incremental_backlog_tracker::rates(const compaction_backlog_source& src) const {
    refresh(src);
    auto stats = src.stats();
    double threshold = src.schema()->min_compaction_threshold();

    // A tier with r runs of size S holds r * S bytes for compaction to read, and is due
    // again once threshold more runs arrive, threshold * S / ingestion seconds later. To
    // end its compaction by then compaction has to read
    //
    //     r * S / (threshold * S / ingestion) = ingestion * r / threshold
    //
    // S cancels: a bigger tier's compaction is longer, but so is its refill. Summed over
    // the tiers with enough runs to compact, _eligible_runs / threshold, this is the rate
    // at which each current compaction ends when the next one of its tier is due. A tier
    // below the threshold has nothing to compact and asks for nothing, so a table whose
    // tree is in shape has a target of 0, and a tier holding more than threshold runs
    // asks for the surplus to be gone by its next deadline.
    //
    // Every flushed byte reaches every tier, so the ingestion is the rate the table
    // flushes at. Only when the window has seen no flush, after a restart or once writes
    // stopped for longer than it covers, does the rate the pending tiers' own data was
    // ingested at, from their sstables, stand in: _pending_rate_from_sstables, the same
    // sum with each tier's inflow in place of the flush rate.
    //
    // How urgent that is depends on who pays for the surplus. A point read touches at
    // most one sstable per run, so with no tier above the threshold it touches at most
    // _ideal_runs sstables; reads touching that many, or more, are paying for every
    // pending run and the sum stands. Reads touching fewer, because the pending runs
    // hold no version of the keys they read, as under unique keys, scale the sum down
    // in proportion, to no less than min_urgency, where the tiers settle at threshold /
    // min_urgency runs instead of threshold. No reads at all, as in a bulk load, are the
    // same as reads touching nothing. Reads touching more than the ideal add nothing:
    // the runs they touch are the pending runs the sum already counts.
    //
    // While a tier is pending the target never drops below the ingestion rate, whatever
    // the reads: every flushed byte is compacted at least once, so the number of sstables
    // stays bounded while compaction is deferred.
    auto sstables_per_read = stats.read_amplification.mean_sstables_read();
    auto urgency = _ideal_runs > 0 ? std::clamp(sstables_per_read / _ideal_runs, min_urgency, 1.0) : min_urgency;
    auto from_ingestion = stats.rates.flushed_bytes * (_eligible_runs > 0 ? std::max(1.0, urgency * (_eligible_runs / threshold)) : 0.0);
    auto from_sstables = _pending_rate_from_sstables * urgency;
    auto target = stats.rates.flushed_bytes.amount() > 0 ? from_ingestion : from_sstables;

    compaction_rates_logger.debug("{}.{}: flushed {:.0f} B/s over {}s, {} runs in {} pending tiers of {} tiers ({} runs, {} at most in a tier), threshold {}, "
            "point reads {:.1f}/s touching {:.2f} sstables of {} ideal, urgency {:.2f}, "
            "from_ingestion {:.0f} B/s, from_sstables {:.0f} B/s over {}s, using {}, target {:.0f} B/s, achieved {:.0f} B/s over {}s",
            src.schema()->ks_name(), src.schema()->cf_name(), stats.rates.flushed_bytes.per_second(), stats.rates.flushed_bytes.interval().count(),
            _eligible_runs, _eligible_tiers, _tiers, _runs, _max_runs_in_tier, threshold,
            stats.read_amplification.point_reads.per_second(), sstables_per_read, _ideal_runs, urgency,
            from_ingestion.per_second(), from_sstables.per_second(), from_sstables.interval().count(),
            stats.rates.flushed_bytes.amount() > 0 ? "from_ingestion" : "from_sstables", target.per_second(),
            stats.rates.compacted_bytes.per_second(), stats.rates.compacted_bytes.interval().count());
    return compaction_rates{
        .target = target,
        .achieved = stats.rates.compacted_bytes,
    };
}

incremental_backlog_tracker::incremental_backlog_tracker(incremental_compaction_strategy_options options) : _options(std::move(options)) {}

double incremental_backlog_tracker::backlog(const compaction_backlog_source& src, const compaction_backlog_tracker::ongoing_writes& ow, const compaction_backlog_tracker::ongoing_compactions& oc) const {
    refresh(src);

    inflight_component compacted = compacted_backlog(oc);

    // Bail out if effective backlog is zero
    if (_total_backlog_bytes <= compacted.total_bytes) {
        return 0;
    }

    // Formula for each SSTable is (Si - Ci) * log(T / Si)
    // Which can be rewritten as: ((Si - Ci) * log(T)) - ((Si - Ci) * log(Si))
    //
    // For the meaning of each variable, please refer to the doc in size_tiered_backlog_tracker.hh

    // Sum of (Si - Ci) for all SSTables contributing backlog
    auto effective_backlog_bytes = _total_backlog_bytes - compacted.total_bytes;

    // Sum of (Si - Ci) * log (Si) for all SSTables contributing backlog
    auto sstables_contribution = _sstables_backlog_contribution - compacted.contribution;
    // This is subtracting ((Si - Ci) * log (Si)) from ((Si - Ci) * log(T)), yielding the final backlog
    auto b = (effective_backlog_bytes * log4(_total_bytes)) - sstables_contribution;
    return b > 0 ? b : 0;
}

void incremental_backlog_tracker::replace_sstables(const std::vector<sstables::shared_sstable>& old_ssts, const std::vector<sstables::shared_sstable>& new_ssts) {
    // Defer backlog contribution recalculation to the next backlog() call,
    // avoiding O(N^2) cost when many sstables are added in a batch (e.g. boot).
    _backlog_dirty = true;
}

}
