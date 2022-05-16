#include "dynamic.hh"

#include <algorithm>
#include <numeric>

using namespace std;
using namespace chrono;
using namespace r2e2;

constexpr seconds SCHEDULING_INTERVAL { 20 };

std::optional<Schedule> DynamicScheduler::schedule(
  const size_t maxWorkers,
  const vector<TreeletStats>& stats,
  const WorkerStats&,
  const size_t )
{
  if ( steady_clock::now() - lastSchedule < SCHEDULING_INTERVAL ) {
    return nullopt;
  }

  lastSchedule = steady_clock::now();

  const size_t treeletCount = stats.size();
  Schedule result( treeletCount, 0 );

  vector<pair<TreeletId, size_t>> waitingBytes;
  waitingBytes.reserve( treeletCount );

  for ( size_t tid = 0; tid < treeletCount; tid++ ) {
    waitingBytes.emplace_back(
      tid, stats[tid].enqueued.bytes - stats[tid].dequeued.bytes );
  }

  sort( waitingBytes.begin(),
        waitingBytes.end(),
        []( const auto& a, const auto& b ) -> bool {
          return a.second > b.second;
        } );

  const size_t totalWaiting = accumulate(
    waitingBytes.begin(),
    waitingBytes.end(),
    0ull,
    []( const auto& res, const auto& a ) { return res + a.second; } );

  if ( totalWaiting == 0 ) {
    return nullopt;
  }

  size_t remainingWorkers = maxWorkers;

  for ( auto& item : waitingBytes ) {
    if ( remainingWorkers == 0 )
      break;

    result[item.first] = static_cast<size_t>(
      ( 1.0 * item.second / totalWaiting ) * maxWorkers );

    if ( result[item.first] == 0 ) {
      result[item.first] = 1;
    }

    remainingWorkers -= result[item.first];
  }

  return { move( result ) };
}
