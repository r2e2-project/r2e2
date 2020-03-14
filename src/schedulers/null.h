#ifndef PBRT_CLOUD_SCHEDULERS_NULL_H
#define PBRT_CLOUD_SCHEDULERS_NULL_H

#include "scheduler.h"

namespace r2t2 {

class NullScheduler : public Scheduler {
  public:
    Optional<Schedule> schedule(const size_t,
                                const std::vector<TreeletStats> &) override {
        return {false};
    }
};

}  // namespace r2t2
#endif /* PBRT_CLOUD_SCHEDULERS_NULL_H */
