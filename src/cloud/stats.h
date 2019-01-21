#ifndef PBRT_CLOUD_STATS_H
#define PBRT_CLOUD_STATS_H

#include <cstdint>
#include <chrono>
#include <ctime>
#include <cstdio>

#include "cloud/manager.h"

namespace pbrt {

/* timing utility functions */
using timepoint_t = std::chrono::time_point<std::chrono::system_clock>;
inline timepoint_t now() {
  return std::chrono::system_clock::now();
};

#define PER_RAY_STATS

const double RAY_PERCENTILES[] = {0.5, 0.9, 0.99, 0.999};
constexpr size_t NUM_PERCENTILES = sizeof(RAY_PERCENTILES) / sizeof(double);

struct RayStats {
    uint64_t finishedPaths{0};
    uint64_t sentRays{0};
    uint64_t receivedRays{0};
    uint64_t rayTraversals{0};

    double traceDurationPercentiles[NUM_PERCENTILES] = {0.0, 0.0, 0.0, 0.0};
    std::vector<double> rayDurations;

    void reset();
    void merge(const RayStats& other);
};

struct WorkerStats {
    RayStats aggregateStats;
    std::map<SceneManager::ObjectTypeID, RayStats> objectStats;

    std::map<std::string, double> timePerAction;
    double totalTime{0};

    timepoint_t start;
    timepoint_t end;

    uint64_t finishedPaths() const { return aggregateStats.finishedPaths; }
    uint64_t sentRays() const { return aggregateStats.sentRays; }
    uint64_t receivedRays() const { return aggregateStats.receivedRays; }
    uint64_t rayTraversals() const { return aggregateStats.receivedRays; }

    void recordFinishedPath();
    void recordSentRay(const SceneManager::ObjectTypeID& type);
    void recordReceivedRay(const SceneManager::ObjectTypeID& type);
    void recordRayTraversal(const SceneManager::ObjectTypeID& type);

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
