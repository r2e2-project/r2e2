#pragma once

#include "allocator.hh"
#include "scheduler.hh"

namespace r2t2 {

class StaticScheduler : public Scheduler
{
private:
  bool scheduledOnce { false };
  Allocator allocator {};

public:
  StaticScheduler( const std::string& path );

  std::optional<Schedule> schedule( const size_t maxWorkers,
                                    const std::vector<TreeletStats>&,
                                    const WorkerStats&,
                                    const size_t ) override;
};

} // namespace r2t2
