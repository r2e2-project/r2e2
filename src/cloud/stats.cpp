#include "stats.h"

namespace pbrt {
namespace global {
WorkerStats workerStats;
}

void RayStats::reset() {
    sentRays = 0;
    receivedRays = 0;
    waitingRays = 0;
    processedRays = 0;
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

    for (int i = 0; i < NUM_PERCENTILES; ++i) {
        traceDurationPercentiles[i] += other.traceDurationPercentiles[i];
    }
#ifdef PER_RAY_STATS
    rayDurations.insert(rayDurations.end(), other.rayDurations.begin(),
                        other.rayDurations.end());
#endif  // PER_RAY_STATS
}

#define INCREMENT_FIELD(name__)          \
    do {                                 \
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

#undef INCREMENT_FIELD

void WorkerStats::reset() {
    _finishedPaths = 0;
    aggregateStats.reset();
    objectStats.clear();
    timePerAction.clear();
    intervalStart = now();
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

    bytesSent = other.bytesSent;
    bytesReceived = other.bytesReceived;
    interval = other.interval;
}

WorkerStats::Recorder::~Recorder() {
    auto end = now();
    stats.timePerAction[name] +=
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            (end - start)).count();
}

WorkerStats::Recorder::Recorder(WorkerStats& stats_, const std::string& name_)
    : stats(stats), name(name_) {
    start = now();
}

}  // namespace pbrt
