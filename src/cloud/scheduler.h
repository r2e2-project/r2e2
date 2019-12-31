#ifndef PBRT_CLOUD_SCHEDULER_H
#define PBRT_CLOUD_SCHEDULER_H

#include <map>
#include <vector>

#include "cloud/lambda.h"
#include "cloud/stats.h"
#include "util/optional.h"

namespace pbrt {

class Scheduler {
  public:
      virtual Optional<std::vector<size_t>> schedule(
        const size_t maxWorkers, const std::vector<TreeletStats> &treelets) = 0;

    virtual ~Scheduler() {}
};

}  // namespace pbrt

#endif /* PBRT_CLOUD_SCHEDULER_H */
