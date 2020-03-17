#ifndef PBRT_CLOUD_SCHEDULERS_UNIFORM_H
#define PBRT_CLOUD_SCHEDULERS_UNIFORM_H

#include "scheduler.hh"

namespace r2t2 {

class UniformScheduler : public Scheduler {
  private:
    bool scheduledOnce{false};

  public:
    Optional<Schedule> schedule(
        const size_t maxWorkers,
        const std::vector<TreeletStats> &treelets) override;
};

}  // namespace r2t2
#endif /* PBRT_CLOUD_SCHEDULERS_UNIFORM_H */
