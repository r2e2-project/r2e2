#ifndef PBRT_CLOUD_SCHEDULERS_STATIC_H
#define PBRT_CLOUD_SCHEDULERS_STATIC_H

#include "allocator.h"
#include "scheduler.h"

namespace r2t2 {

class StaticScheduler : public Scheduler {
  private:
    bool scheduledOnce{false};
    Allocator allocator;

  public:
    StaticScheduler(const std::string &path);

    Optional<Schedule> schedule(const size_t maxWorkers,
                                const std::vector<TreeletStats> &) override;
};

}  // namespace r2t2
#endif /* PBRT_CLOUD_SCHEDULERS_STATIC_H */
