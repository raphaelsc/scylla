/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2016 ScyllaDB
 *
 * Modified by ScyllaDB
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

#include <map>
#include <chrono>
#include <algorithm>
#include <vector>
#include <iterator>
#include "sstables.hh"
#include "compaction.hh"

class date_tiered_manifest {
    logging::logger logger;

    static constexpr double DEFAULT_MAX_SSTABLE_AGE_DAYS = 365;
    static constexpr uint64_t DEFAULT_BASE_TIME_SECONDS = 60;

    // TODO: implement date_tiered_compaction_strategy_options.
    uint64_t _max_sstable_age;
    uint64_t _base_time;
public:
    date_tiered_manifest() = delete;

    date_tiered_manifest(const std::map<sstring, sstring>& options)
        : logger("DateTieredCompactionStrategy") {
        // NOTE: Timestamp resolution used here is microseconds.
        std::chrono::seconds s(DEFAULT_BASE_TIME_SECONDS);
        _base_time = std::chrono::duration_cast<std::chrono::microseconds>(s).count();
        std::chrono::hours h(24);
        _max_sstable_age = DEFAULT_MAX_SSTABLE_AGE_DAYS * std::chrono::duration_cast<std::chrono::microseconds>(h).count();

        // FIXME: implement option to disable tombstone compaction.
#if 0
        if (!options.containsKey(AbstractCompactionStrategy.TOMBSTONE_COMPACTION_INTERVAL_OPTION) && !options.containsKey(AbstractCompactionStrategy.TOMBSTONE_THRESHOLD_OPTION))
        {
            disableTombstoneCompactions = true;
            logger.debug("Disabling tombstone compactions for DTCS");
        }
        else
            logger.debug("Enabling tombstone compactions for DTCS");
#endif
    }

    /**
     *
     * @param gcBefore
     * @return
     */
    std::vector<sstables::shared_sstable>
    get_next_sstables(column_family& cfs, std::vector<sstables::shared_sstable>& uncompacting, int32_t gc_before) {
        if (cfs.get_sstables()->empty()) {
            return {};
        }

        // Find fully expired SSTables. Those will be included no matter what.
        auto expired = get_fully_expired_sstables(cfs, uncompacting, gc_before);

        auto sort_ssts = [] (std::vector<sstables::shared_sstable>& sstables) {
            std::sort(sstables.begin(), sstables.end(), [] (const auto& x, const auto& y) {
                return x->generation() < y->generation();
            });
        };
        sort_ssts(uncompacting);
        sort_ssts(expired);

        std::vector<sstables::shared_sstable> non_expired_set;
        // Set non_expired_set with the elements that are in uncompacting, but not in the expired.
        std::set_difference(uncompacting.begin(), uncompacting.end(), expired.begin(), expired.end(),
            std::inserter(non_expired_set, non_expired_set.begin()), [] (const auto& x, const auto& y) {
                return x->generation() < y->generation();
            });

        auto compaction_candidates = get_next_non_expired_sstables(cfs, non_expired_set, gc_before);
        if (!expired.empty()) {
            compaction_candidates.insert(compaction_candidates.end(), expired.begin(), expired.end());
        }
        return compaction_candidates;
    }

private:
    std::vector<sstables::shared_sstable>
    get_next_non_expired_sstables(column_family& cfs, std::vector<sstables::shared_sstable>& non_expiring_sstables, int32_t gc_before) {
        int base = cfs.schema()->min_compaction_threshold();
        int64_t now = get_now(cfs);
        auto most_interesting = get_compaction_candidates(cfs, non_expiring_sstables, now, base);

        return most_interesting;

        // FIXME: implement functionality below that will look for a single sstable with worth dropping tombstone,
        // iff strategy didn't find anything to compact. So it's not essential.
#if 0
        // if there is no sstable to compact in standard way, try compacting single sstable whose droppable tombstone
        // ratio is greater than threshold.

        List<SSTableReader> sstablesWithTombstones = Lists.newArrayList();
        for (SSTableReader sstable : nonExpiringSSTables)
        {
            if (worthDroppingTombstones(sstable, gcBefore))
                sstablesWithTombstones.add(sstable);
        }
        if (sstablesWithTombstones.isEmpty())
            return Collections.emptyList();

        return Collections.singletonList(Collections.min(sstablesWithTombstones, new SSTableReader.SizeComparator()));
#endif
    }

    std::vector<sstables::shared_sstable>
    get_compaction_candidates(column_family& cfs, std::vector<sstables::shared_sstable> candidate_sstables, int64_t now, int base) {
        int min_threshold = cfs.schema()->min_compaction_threshold();
        int max_threshold = cfs.schema()->max_compaction_threshold();
        auto candidates = filter_old_sstables(candidate_sstables, _max_sstable_age, now);

        auto buckets = get_buckets(create_sst_and_min_timestamp_pairs(candidates), _base_time, base, now);

        return newest_bucket(buckets, min_threshold, max_threshold, now, _base_time);
    }

    /**
     * Gets the timestamp that DateTieredCompactionStrategy considers to be the "current time".
     * @return the maximum timestamp across all SSTables.
     */
    static int64_t get_now(column_family& cfs) {
        int64_t max_timestamp = 0;
        for (auto& entry : *cfs.get_sstables()) {
            auto& sst = entry.second;
            int64_t candidate = sst->get_stats_metadata().max_timestamp;
            max_timestamp = candidate > max_timestamp ? candidate : max_timestamp;
        }
        return max_timestamp;
    }

    /**
     * Removes all sstables with max timestamp older than maxSSTableAge.
     * @param sstables all sstables to consider
     * @param maxSSTableAge the age in milliseconds when an SSTable stops participating in compactions
     * @param now current time. SSTables with max timestamp less than (now - maxSSTableAge) are filtered.
     * @return a list of sstables with the oldest sstables excluded
     */
    static std::vector<sstables::shared_sstable>
    filter_old_sstables(std::vector<sstables::shared_sstable> sstables, int64_t max_sstable_age, int64_t now) {
        if (max_sstable_age == 0) {
            return sstables;
        }
        assert((now - max_sstable_age) > 0);
        int64_t cutoff = now - max_sstable_age;

        sstables.erase(std::remove_if(sstables.begin(), sstables.end(), [cutoff] (auto& sst) {
            return sst->get_stats_metadata().max_timestamp < cutoff;
        }), sstables.end());

        return sstables;
    }

    /**
     *
     * @param sstables
     * @return
     */
    static std::vector<std::pair<sstables::shared_sstable,int64_t>>
    create_sst_and_min_timestamp_pairs(const std::vector<sstables::shared_sstable>& sstables) {
        std::vector<std::pair<sstables::shared_sstable,int64_t>> sstable_min_timestamp_pairs;
        sstable_min_timestamp_pairs.reserve(sstables.size());
        for (auto& sst : sstables) {
            sstable_min_timestamp_pairs.emplace_back(sst, sst->get_stats_metadata().min_timestamp);
        }
        return sstable_min_timestamp_pairs;
    }

    /**
     * A target time span used for bucketing SSTables based on timestamps.
     */
    struct target {
        // How big a range of timestamps fit inside the target.
        int64_t size;
        // A timestamp t hits the target iff t / size == divPosition.
        int64_t div_position;

        target() = delete;
        target(int64_t size, int64_t div_position) : size(size), div_position(div_position) {}

        /**
         * Compares the target to a timestamp.
         * @param timestamp the timestamp to compare.
         * @return a negative integer, zero, or a positive integer as the target lies before, covering, or after than the timestamp.
         */
        int compare_to_timestamp(int64_t timestamp) {
            auto ts1 = div_position;
            auto ts2 = timestamp / size;
            return (ts1 > ts2 ? 1 : (ts1 == ts2 ? 0 : -1));
        }

        /**
         * Tells if the timestamp hits the target.
         * @param timestamp the timestamp to test.
         * @return <code>true</code> iff timestamp / size == divPosition.
         */
        bool on_target(int64_t timestamp) {
            return compare_to_timestamp(timestamp) == 0;
        }

        /**
         * Gets the next target, which represents an earlier time span.
         * @param base The number of contiguous targets that will have the same size. Targets following those will be <code>base</code> times as big.
         * @return
         */
        target next_target(int base)
        {
            if (div_position % base > 0) {
                return target(size, div_position - 1);
            } else {
                return target(size * base, div_position / base - 1);
            }
        }
    };


    /**
     * Group files with similar min timestamp into buckets. Files with recent min timestamps are grouped together into
     * buckets designated to short timespans while files with older timestamps are grouped into buckets representing
     * longer timespans.
     * @param files pairs consisting of a file and its min timestamp
     * @param timeUnit
     * @param base
     * @param now
     * @return a list of buckets of files. The list is ordered such that the files with newest timestamps come first.
     *         Each bucket is also a list of files ordered from newest to oldest.
     */
    std::vector<std::vector<sstables::shared_sstable>>
    get_buckets(std::vector<std::pair<sstables::shared_sstable,int64_t>>&& files, int64_t time_unit, int base, int64_t now) {
        // Sort files by age. Newest first.
        std::sort(files.begin(), files.end(), [] (auto& i, auto& j) {
            return i.second > j.second;
        });

        std::vector<std::vector<sstables::shared_sstable>> buckets;
        auto target = get_initial_target(now, time_unit);
        auto it = files.begin();

        while (it != files.end()) {
            bool finish = false;
            while (!target.on_target(it->second)) {
                // If the file is too new for the target, skip it.
                if (target.compare_to_timestamp(it->second) < 0) {
                    it++;
                    if (it == files.end()) {
                        finish = true;
                        break;
                    }
                } else { // If the file is too old for the target, switch targets.
                    target = target.next_target(base);
                }
            }
            if (finish) {
                break;
            }

            std::vector<sstables::shared_sstable> bucket;
            while (target.on_target(it->second)) {
                bucket.push_back(it->first);
                it++;
                if (it == files.end()) {
                    break;
                }
            }
            buckets.push_back(bucket);
        }

        return buckets;
    }

    target get_initial_target(uint64_t now, int64_t time_unit) {
        return target(time_unit, now / time_unit);
    }

    /**
     * @param buckets list of buckets, sorted from newest to oldest, from which to return the newest bucket within thresholds.
     * @param minThreshold minimum number of sstables in a bucket to qualify.
     * @param maxThreshold maximum number of sstables to compact at once (the returned bucket will be trimmed down to this).
     * @return a bucket (list) of sstables to compact.
     */
    std::vector<sstables::shared_sstable>
    newest_bucket(std::vector<std::vector<sstables::shared_sstable>>& buckets, int min_threshold, int max_threshold, int64_t now, int64_t base_time) {
        // If the "incoming window" has at least minThreshold SSTables, choose that one.
        // For any other bucket, at least 2 SSTables is enough.
        // In any case, limit to maxThreshold SSTables.
        target incoming_window = get_initial_target(now, base_time);
        for (auto& bucket : buckets) {
            auto min_timestamp = bucket.front()->get_stats_metadata().min_timestamp;
            if (bucket.size() >= size_t(min_threshold) ||
                    (bucket.size() >= 2 && !incoming_window.on_target(min_timestamp))) {
                trim_to_threshold(bucket, max_threshold);
                return bucket;
            }
        }
        return {};
    }


    /**
     * @param bucket list of sstables, ordered from newest to oldest by getMinTimestamp().
     * @param maxThreshold maximum number of sstables in a single compaction task.
     * @return A bucket trimmed to the <code>maxThreshold</code> newest sstables.
     */
    static void trim_to_threshold(std::vector<sstables::shared_sstable>& bucket, int max_threshold) {
        // Trim the oldest sstables off the end to meet the maxThreshold
        bucket.resize(std::min(bucket.size(), size_t(max_threshold)));
    }
};
