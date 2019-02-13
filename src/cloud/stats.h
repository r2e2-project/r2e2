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
#define PER_INTERVAL_STATS
#define RECORD_METRICS

const double RAY_PERCENTILES[] = {0.5, 0.9, 0.99, 0.999, 0.9999};
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
    /* rays that require (or required) this scence object to render */
    uint64_t demandedRays{0};

    double traceDurationPercentiles[NUM_PERCENTILES] = {0.0, 0.0, 0.0, 0.0,
                                                        0.0};
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
    uint64_t outstandingUdp{0};
};

struct WorkerStats {
    /* required stats */
    uint64_t _finishedPaths{0};
    RayStats aggregateStats;
    QueueStats queueStats;
    std::map<SceneManager::ObjectKey, RayStats> objectStats;

    /* diagnostic stats */
    std::map<std::string, double> timePerAction;
    std::map<std::string, std::vector<std::tuple<uint64_t, uint64_t>>>
        intervalsPerAction;
    std::map<std::string, std::vector<std::tuple<uint64_t, double>>>
        metricsOverTime;

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
    void recordRayInterval(const SceneManager::ObjectKey& type,
                           timepoint_t start, timepoint_t end);
    void recordDemandedRay(const SceneManager::ObjectKey& type);

    void reset();
    void resetDiagnostics();

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

    void recordMetric(const std::string& name, timepoint_t time, double metric);
};

// Ray statistics, but with doubles for numerical stability
struct RayStatsD {
    /* rays sent to this scene object */
    double sentRays{0};
    /* rays received for this scene object */
    double receivedRays{0};
    /* rays waiting to be processed for this scene object */
    double waitingRays{0};
    /* rays processed for this scene object */
    double processedRays{0};
    /* rays that require (or required) this scence object to render */
    double demandedRays{0};

    void reset();

    RayStatsD()
        : sentRays(0),
          receivedRays(0),
          waitingRays(0),
          processedRays(0),
          demandedRays(0) {}

    RayStatsD(double sentRays, double receivedRays, double waitingRays,
              double processedRays, double demandedRays)
        : sentRays(sentRays),
          receivedRays(receivedRays),
          waitingRays(waitingRays),
          processedRays(processedRays),
          demandedRays(demandedRays) {}
    RayStatsD(const RayStats& other)
        : sentRays(other.sentRays),
          receivedRays(other.receivedRays),
          waitingRays(other.waitingRays),
          processedRays(other.processedRays),
          demandedRays(other.demandedRays) {}
};

RayStatsD operator+(const RayStatsD& a, const RayStatsD& b);

RayStatsD operator*(const RayStatsD& a, double scalar);

RayStatsD operator*(double scalar, const RayStatsD& a);

struct RayStatsPerObjectD {
    std::map<SceneManager::ObjectKey, RayStatsD> stats;
    RayStatsPerObjectD();
    RayStatsPerObjectD(const WorkerStats& full);
};

RayStatsPerObjectD operator+(const RayStatsPerObjectD& a,
                             const RayStatsPerObjectD& b);

RayStatsPerObjectD operator*(const RayStatsPerObjectD& a, double scalar);

RayStatsPerObjectD operator*(double scalar, const RayStatsPerObjectD& a);

namespace global {
extern WorkerStats workerStats;
}

}  // namespace pbrt

#endif /* PBRT_CLOUD_STATS_H */
