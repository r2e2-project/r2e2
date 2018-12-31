#ifndef PBRT_CLOUD_STATS_H
#define PBRT_CLOUD_STATS_H

#include <cstdint>

namespace pbrt {

struct WorkerStats {
    uint64_t finishedPaths{0};

    void reset() {
        finishedPaths = 0;
    }
};

namespace global {
extern WorkerStats workerStats;
}

}  // namespace pbrt

#endif /* PBRT_CLOUD_STATS_H */
