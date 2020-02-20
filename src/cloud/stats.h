#ifndef PBRT_CLOUD_STATS_H
#define PBRT_CLOUD_STATS_H

#include <cmath>
#include <cstdint>
#include <unordered_map>

#include "cloud/estimators.h"
#include "cloud/lambda.h"
#include "cloud/manager.h"
#include "cloud/raystate.h"

namespace pbrt {

/* timing utility functions */
using timepoint_t = std::chrono::time_point<std::chrono::system_clock>;
inline timepoint_t now() { return std::chrono::system_clock::now(); };

struct TreeletStats {
    struct Stats {
        uint64_t rays{0};
        uint64_t bytes{0};
        uint64_t count{0};
    };

    Stats enqueued{}, dequeued{};
    std::vector<Stats> enqueuedTo, dequeuedFrom;

    void merge(const TreeletStats& other);
    void reset();

    TreeletStats operator-(const TreeletStats& other) const;
};

struct WorkerStats {
    uint64_t finishedPaths{0};

    struct {
        uint64_t rays{0};
        uint64_t bytes{0};
        uint64_t count{0};
    } enqueued{}, assigned{}, dequeued{}, samples{};

    void merge(const WorkerStats& other);
    void reset();

    WorkerStats operator-(const WorkerStats& other) const;
};

}  // namespace pbrt

#endif /* PBRT_CLOUD_STATS_H */
