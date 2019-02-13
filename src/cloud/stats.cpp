#include "stats.h"

#include <math.h>
#include <iomanip>
#include <iostream>

using namespace std::chrono;
using namespace std;

namespace pbrt {
namespace global {
WorkerStats workerStats;
}

void RayStats::reset() {
    sentRays = 0;
    receivedRays = 0;
    waitingRays = 0;
    processedRays = 0;
    demandedRays = 0;
    for (double& d : traceDurationPercentiles) {
        d = 0;
    }

#ifdef PER_RAY_STATS
    rayDurations.clear();
#endif  // PER_RAY_STATS
}

void RayStats::merge(const RayStats& other) {
    sentRays += other.sentRays;
    receivedRays += other.receivedRays;
    waitingRays += other.waitingRays;
    processedRays += other.processedRays;
    demandedRays += other.demandedRays;

    for (int i = 0; i < NUM_PERCENTILES; ++i) {
        traceDurationPercentiles[i] += other.traceDurationPercentiles[i];
    }
#ifdef PER_RAY_STATS
    rayDurations.insert(rayDurations.end(), other.rayDurations.begin(),
                        other.rayDurations.end());
#endif  // PER_RAY_STATS
}

#define INCREMENT_FIELD(name__)        \
    do {                               \
        aggregateStats.name__ += 1;    \
        objectStats[type].name__ += 1; \
    } while (false)

void WorkerStats::recordFinishedPath() { _finishedPaths += 1; }

void WorkerStats::recordSentRay(const SceneManager::ObjectKey& type) {
    INCREMENT_FIELD(sentRays);
}

void WorkerStats::recordReceivedRay(const SceneManager::ObjectKey& type) {
    INCREMENT_FIELD(receivedRays);
}
void WorkerStats::recordWaitingRay(const SceneManager::ObjectKey& type) {
    INCREMENT_FIELD(waitingRays);
}
void WorkerStats::recordProcessedRay(const SceneManager::ObjectKey& type) {
    INCREMENT_FIELD(processedRays);
}
void WorkerStats::recordDemandedRay(const SceneManager::ObjectKey& type) {
    INCREMENT_FIELD(demandedRays);
}

#undef INCREMENT_FIELD

void WorkerStats::recordRayInterval(const SceneManager::ObjectKey& type,
                                    timepoint_t start, timepoint_t end) {
    auto total_time =
        std::chrono::duration_cast<std::chrono::nanoseconds>((end - start))
            .count();
    aggregateStats.rayDurations.push_back(total_time);
#ifdef PER_RAY_STATS
    objectStats[type].rayDurations.push_back(total_time);
#endif
}

void WorkerStats::reset() {
    _finishedPaths = 0;
    aggregateStats.reset();
    objectStats.clear();
}

void WorkerStats::resetDiagnostics() {
    reset();
    timePerAction.clear();
    intervalStart = now();
    intervalsPerAction.clear();
    intervalStart = now();
    intervalsPerAction.clear();
    metricsOverTime.clear();
}

void WorkerStats::merge(const WorkerStats& other) {
    _finishedPaths += other._finishedPaths;
    aggregateStats.merge(other.aggregateStats);
    queueStats = other.queueStats;
    for (const auto& kv : other.objectStats) {
        objectStats[kv.first].merge(kv.second);
    }
    for (const auto& kv : other.timePerAction) {
        timePerAction[kv.first] += kv.second;
    }
    for (const auto& kv : other.intervalsPerAction) {
        intervalsPerAction[kv.first].insert(intervalsPerAction[kv.first].end(),
                                            kv.second.begin(), kv.second.end());
    }
    for (const auto& kv : other.metricsOverTime) {
        metricsOverTime[kv.first].insert(metricsOverTime[kv.first].end(),
                                         kv.second.begin(), kv.second.end());
    }
}

WorkerStats::Recorder::~Recorder() {
    auto end = now();
    stats.timePerAction[name] +=
        std::chrono::duration_cast<std::chrono::nanoseconds>((end - start))
            .count();
#ifdef PER_INTERVAL_STATS
    stats.intervalsPerAction[name].push_back(
        std::make_tuple(std::chrono::duration_cast<std::chrono::nanoseconds>(
                            (start - stats.intervalStart))
                            .count(),
                        std::chrono::duration_cast<std::chrono::nanoseconds>(
                            (end - stats.intervalStart))
                            .count()));
#endif
}

WorkerStats::Recorder::Recorder(WorkerStats& stats_, const std::string& name_)
    : stats(stats_), name(name_) {
    start = now();
}

void WorkerStats::recordMetric(const std::string& name, timepoint_t time,
                               double metric) {
    metricsOverTime[name].push_back(std::make_tuple(
        (uint64_t)std::chrono::duration_cast<std::chrono::nanoseconds>(
            time - intervalStart)
            .count(),
        metric));
}

RayStatsD operator+(const RayStatsD& a, const RayStatsD& b) {
    return RayStatsD{
        a.sentRays + b.sentRays,         a.receivedRays + b.receivedRays,
        a.waitingRays + b.waitingRays,   a.processedRays + b.processedRays,
        a.demandedRays + b.demandedRays,
    };
}

RayStatsD operator*(const RayStatsD& a, double scalar) {
    return RayStatsD{
        a.sentRays * scalar,     a.receivedRays * scalar,
        a.waitingRays * scalar,  a.processedRays * scalar,
        a.demandedRays * scalar,
    };
}

RayStatsD operator*(double scalar, const RayStatsD& a) { return a * scalar; }

void RayStatsD::reset() {
    sentRays = 0.0;
    receivedRays = 0.0;
    waitingRays = 0.0;
    processedRays = 0.0;
    demandedRays = 0.0;
}

RayStatsPerObjectD::RayStatsPerObjectD() {}
RayStatsPerObjectD::RayStatsPerObjectD(const WorkerStats& full) {
    for (const auto& kv : full.objectStats) {
        stats.insert(make_pair(kv.first, RayStatsD{kv.second}));
    }
}

RayStatsPerObjectD operator+(const RayStatsPerObjectD& a,
                             const RayStatsPerObjectD& b) {
    RayStatsPerObjectD n{a};
    for (const auto& kv : b.stats) {
        n.stats[kv.first] = n.stats[kv.first] + kv.second;
    }
    return n;
}

RayStatsPerObjectD operator*(const RayStatsPerObjectD& a, double scalar) {
    RayStatsPerObjectD n{a};
    for (auto& kv : n.stats) {
        kv.second = kv.second * scalar;
    }
    return n;
}

RayStatsPerObjectD operator*(double scalar, const RayStatsPerObjectD& a) {
    return a * scalar;
}
}  // namespace pbrt
