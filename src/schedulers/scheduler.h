#ifndef PBRT_CLOUD_SCHEDULER_H
#define PBRT_CLOUD_SCHEDULER_H

#include <map>
#include <vector>

#include "common/lambda.h"
#include "common/stats.h"
#include "util/optional.h"

namespace r2t2 {

/* Schedule is a {TreeletId -> Worker Count} mapping */
using Schedule = std::vector<size_t>;

class Scheduler {
  public:
    virtual Optional<Schedule> schedule(
        const size_t maxWorkers, const std::vector<TreeletStats> &treelets) = 0;

    virtual ~Scheduler() {}
};

}  // namespace r2t2

#endif /* PBRT_CLOUD_SCHEDULER_H */
