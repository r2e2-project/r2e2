#pragma once

#include "scheduler.hh"

namespace r2t2 {

class NullScheduler : public Scheduler
{
public:
  Optional<Schedule> schedule( const size_t,
                               const std::vector<TreeletStats>& ) override
  {
    return { false };
  }
};

} // namespace r2t2
