#include "stats.h"

namespace pbrt {
namespace global {
WorkerStats workerStats;
}

void RayStats::reset() {
    finishedPaths = 0;
    sentRays = 0;
    receivedRays = 0;
    for (double& d : traceDurationPercentiles) {
        d = 0;
    }
#ifdef PER_RAY_STATS
    rayDurations.clear();
#endif  // PER_RAY_STATS
}

void RayStats::merge(const RayStats& other) {
    finishedPaths += other.finishedPaths;
    sentRays += other.sentRays;
    receivedRays += other.receivedRays;
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

void WorkerStats::recordFinishedPath() { aggregateStats.finishedPaths += 1; }

void WorkerStats::recordSentRay(const SceneManager::ObjectTypeID& type) {
    INCREMENT_FIELD(sentRays);
}
void WorkerStats::recordReceivedRay(const SceneManager::ObjectTypeID& type) {
    INCREMENT_FIELD(receivedRays);
}
void WorkerStats::recordRayTraversal(const SceneManager::ObjectTypeID& type) {
    INCREMENT_FIELD(rayTraversals);
}

#undef INCREMENT_FIELD

void WorkerStats::reset() {
    aggregateStats.reset();
    objectStats.clear();
    timePerAction.clear();
    totalTime = 0;
}

void WorkerStats::merge(const WorkerStats& other) {
    aggregateStats.merge(other.aggregateStats);
    for (const auto& kv : other.objectStats) {
        objectStats[kv.first].merge(kv.second);
    }
    for (const auto& kv : other.timePerAction) {
        timePerAction[kv.first] += kv.second;
    }
    totalTime += other.totalTime;
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
