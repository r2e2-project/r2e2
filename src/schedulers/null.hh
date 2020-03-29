#pragma once

#include "scheduler.hh"

namespace r2t2 {

class NullScheduler : public Scheduler
{
public:
  std::optional<Schedule> schedule( const size_t,
                                    const std::vector<TreeletStats>& ) override
  {
    return std::nullopt;
  }
};

} // namespace r2t2
