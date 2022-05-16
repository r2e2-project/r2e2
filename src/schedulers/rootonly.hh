#pragma once

#include "scheduler.hh"

namespace r2e2 {

class RootOnlyScheduler : public Scheduler
{
private:
  bool scheduledOnce { false };

public:
  std::optional<Schedule> schedule( const size_t maxWorkers,
                                    const std::vector<TreeletStats>& treelets,
                                    const WorkerStats&,
                                    const size_t ) override
  {
    if ( scheduledOnce ) {
      return std::nullopt;
    }

    scheduledOnce = true;

    Schedule result( treelets.size(), 0 );
    result[0] = maxWorkers;
    return { std::move( result ) };
  }
};

} // namespace r2e2
