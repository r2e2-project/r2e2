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
      last_scheduled_at_ = steady_clock::now();
      stage_ = Stage::PERCENT_0;
      last_worker_count_ = max_workers;
      StaticScheduler static_scheduler { path_ };
      last_schedule_ = *static_scheduler.schedule( max_workers, treelets, stats, n_paths );
      return last_schedule_;
    }

    case Stage::PERCENT_0:
      if ( steady_clock::now() - last_scheduled_at_ >= SCHEDULING_INTERVAL ) {
        last_scheduled_at_ = steady_clock::now();

        /* now, let's see if we're over provisioning */
        for ( size_t tid = 0; tid < treelets.size(); tid++ ) {
          auto& treelet = treelets[tid];
          const auto outstanding
            = treelet.enqueued.rays - treelet.dequeued.rays;
          auto& count = last_schedule_[tid];

          while ( count > 1 and count * WORKER_MAX_ACTIVE_RAYS > 8 * outstanding ) {
            count--;
          }
        }

        return last_schedule_;
      }

      break;

    case Stage::PERCENT_95:
      break;
  }

  return nullopt;
}
