#pragma once

#include "scheduler.hh"

namespace r2e2 {

class NullScheduler : public Scheduler
{
private:
  bool scheduled_once_ { false };

public:
  std::optional<Schedule> schedule( const size_t,
                                    const std::vector<TreeletStats>& treelets,
                                    const WorkerStats&,
                                    const size_t ) override
  {
    if ( scheduled_once_ ) {
      return std::nullopt;
    }

    scheduled_once_ = true;

    Schedule result( treelets.size(), 0 );
    return { std::move( result ) };
  }
};

} // namespace r2e2
