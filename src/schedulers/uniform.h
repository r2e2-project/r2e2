#ifndef PBRT_CLOUD_SCHEDULERS_UNIFORM_H
#define PBRT_CLOUD_SCHEDULERS_UNIFORM_H

#include "cloud/scheduler.h"

namespace pbrt {

class UniformScheduler : public Scheduler {
  private:
    bool scheduledOnce{false};

  public:
    Optional<Schedule> schedule(
        const size_t maxWorkers,
        const std::vector<TreeletStats> &treelets) override;
};

}  // namespace pbrt
#endif /* PBRT_CLOUD_SCHEDULERS_UNIFORM_H */
