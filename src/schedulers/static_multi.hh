#pragma once

#include "allocator.hh"
#include "scheduler.hh"

namespace r2t2 {

class StaticMultiScheduler : public Scheduler
{
private:
  Allocator allocator {};

public:
  StaticMultiScheduler( const std::string& path );

  std::optional<Schedule> schedule( const size_t maxWorkers,
                                    const std::vector<TreeletStats>&,
                                    const WorkerStats&,
                                    const size_t ) override;
};

} // namespace r2t2
