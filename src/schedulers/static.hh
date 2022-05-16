#pragma once

#include <valarray>

#include "scheduler.hh"

namespace r2e2 {

class StaticScheduler : public Scheduler
{
private:
  bool scheduled_once_ { false };

protected:
  std::valarray<double> weights_ {};
  Schedule get_schedule( size_t max_workers, const size_t treelet_count ) const;

public:
  StaticScheduler( const std::string& path );

  std::optional<Schedule> schedule( const size_t max_workers,
                                    const std::vector<TreeletStats>&,
                                    const WorkerStats&,
                                    const size_t ) override;
};

} // namespace r2e2
