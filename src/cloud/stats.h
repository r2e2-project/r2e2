#ifndef PBRT_CLOUD_STATS_H
#define PBRT_CLOUD_STATS_H

#include <chrono>
#include <cstdint>

#include "cloud/manager.h"

namespace pbrt {

/* timing utility functions */
using timepoint_t = std::chrono::time_point<std::chrono::system_clock>;
inline timepoint_t now() { return std::chrono::system_clock::now(); };

#define PER_RAY_STATS

const double RAY_PERCENTILES[] = {0.5, 0.9, 0.99, 0.999};
constexpr size_t NUM_PERCENTILES = sizeof(RAY_PERCENTILES) / sizeof(double);

struct RayStats {
    /* rays sent to this scene object */
    uint64_t sentRays{0};
    /* rays received for this scene object */
    uint64_t receivedRays{0};
    /* rays waiting to be processed for this scene object */
    uint64_t waitingRays{0};
    /* rays processed for this scene object */
    uint64_t processedRays{0};

    double traceDurationPercentiles[NUM_PERCENTILES] = {0.0, 0.0, 0.0, 0.0};
    std::vector<double> rayDurations;

    void reset();
    void merge(const RayStats& other);
};

struct QueueStats {
    uint64_t ray{0};
    uint64_t finished{0};
    uint64_t pending{0};
    uint64_t out{0};
    uint64_t connecting{0};
    uint64_t connected{0};
};

struct WorkerStats {
    uint64_t _finishedPaths{0};

    RayStats aggregateStats;
    QueueStats queueStats;
    std::map<SceneManager::ObjectKey, RayStats> objectStats;

    std::map<std::string, double> timePerAction;

    uint64_t bytesSent{0};
    uint64_t bytesReceived{0};
    std::chrono::milliseconds interval;

    timepoint_t intervalStart{now()};

    uint64_t finishedPaths() const { return _finishedPaths; }
    uint64_t sentRays() const { return aggregateStats.sentRays; }
    uint64_t receivedRays() const { return aggregateStats.receivedRays; }
    uint64_t waitingRays() const { return aggregateStats.waitingRays; }
    uint64_t processedRays() const { return aggregateStats.processedRays; }

    void recordFinishedPath();
    void recordSentRay(const SceneManager::ObjectKey& type);
    void recordReceivedRay(const SceneManager::ObjectKey& type);
    void recordWaitingRay(const SceneManager::ObjectKey& type);
    void recordProcessedRay(const SceneManager::ObjectKey& type);

    void reset();

    void merge(const WorkerStats& other);

    /* for recording action intervals */
    struct Recorder {
        ~Recorder();

      private:
        friend WorkerStats;

        Recorder(WorkerStats& stats_, const std::string& name_);

        WorkerStats& stats;
        std::string name;
        timepoint_t start;
    };

    friend Recorder;
    Recorder recordInterval(const std::string& name) {
        return Recorder(*this, name);
    }
};

namespace global {
extern WorkerStats workerStats;
}

}  // namespace pbrt

#endif /* PBRT_CLOUD_STATS_H */
