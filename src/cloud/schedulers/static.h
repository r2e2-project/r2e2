#ifndef PBRT_CLOUD_SCHEDULERS_NULL_H
#define PBRT_CLOUD_SCHEDULERS_NULL_H

#include "cloud/allocator.h"
#include "cloud/scheduler.h"

namespace pbrt {

class StaticScheduler : public Scheduler {
  private:
    bool scheduledOnce{false};
    Allocator allocator;

  public:
    StaticScheduler(const std::string &path);

    Optional<Schedule> schedule(const size_t maxWorkers,
                                const std::vector<TreeletStats> &) override;
};

}  // namespace pbrt
#endif /* PBRT_CLOUD_SCHEDULERS_NULL_H */
