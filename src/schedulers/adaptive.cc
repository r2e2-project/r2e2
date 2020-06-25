#include "adaptive.hh"

#include "common/lambda.hh"
#include "static.hh"

using namespace std;
using namespace chrono;
using namespace r2t2;

constexpr seconds SCHEDULING_INTERVAL { 60 };

optional<Schedule> AdaptiveScheduler::schedule(
  const size_t max_workers,
  const vector<TreeletStats>& treelets,
  const WorkerStats& stats,
  const size_t n_paths )
{
  switch ( stage_ ) {
    case Stage::INITIAL: {
      last_schedule_ = steady_clock::now();
      stage_ = Stage::PERCENT_0;
      last_worker_count_ = max_workers;
      StaticScheduler static_scheduler { path_ };
      return static_scheduler.schedule( max_workers, treelets, stats, n_paths );
    }

    case Stage::PERCENT_0:
      if ( steady_clock::now() - last_schedule_ >= SCHEDULING_INTERVAL ) {
        last_schedule_ = steady_clock::now();

        StaticScheduler static_scheduler { path_ };
        auto schedule = static_scheduler.schedule(
          last_worker_count_, treelets, stats, n_paths );

        if ( not schedule ) {
          throw runtime_error( "invalid static schedule" );
        }

        /* now, let's see if we're over provisioning */
        for ( size_t tid = 0; tid < treelets.size(); tid++ ) {
          auto& treelet = treelets[tid];
          const auto outstanding
            = treelet.enqueued.rays - treelet.dequeued.rays;
          auto& count = schedule->at( tid );

          while ( count > 1 and count * WORKER_MAX_ACTIVE_RAYS > outstanding ) {
            count--;
          }
        }

        return schedule;
      }

      break;

    case Stage::PERCENT_95:
      break;
  }

  return nullopt;
}
