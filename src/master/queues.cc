#include "lambda-master.hh"
#include "messages/utils.hh"
#include "util/random.hh"

#include "parallel.h"

using namespace std;
using namespace r2t2;
using namespace meow;

using OpCode = Message::OpCode;

pair<bool, bool> LambdaMaster::assign_work( Worker& worker )
{
  /* return, if worker is not active anymore */
  if ( worker.state != Worker::State::Active )
    return { false, false };

  /* if the worker's role is accumulator */
  if ( worker.role == Worker::Role::Accumulator ) {
    if ( worker.active_rays() >= WORKER_MAX_ACTIVE_SAMPLES )
      return { false, false };

    /* Q1: do we have any sample for this guy? */
    const TileId tile_id = worker.tile_id;
    auto& bag_queue = queued_ray_bags[treelet_count + tile_id];

    bool work_to_do = ( bag_queue.size() > 0 );

    if ( not work_to_do ) {
      return { true, false };
    }

    if ( worker.active_rays() < WORKER_MAX_ACTIVE_SAMPLES ) {
      worker.to_be_assigned.push_back( bag_queue.front() );
      record_assign( worker, bag_queue.front() );

      bag_queue.pop();
      queued_ray_bags_count--;

      return { worker.active_rays() < WORKER_MAX_ACTIVE_SAMPLES, true };
    } else {
      return { false, false };
    }
  }

  // at this point, we know the worker is not an accumulator

  /* return if the worker already has enough work */
  if ( worker.active_rays() >= WORKER_MAX_ACTIVE_RAYS )
    return { false, false };

  /* return, if the worker doesn't have any treelets */
  if ( not worker.treelet )
    return { false, false };

  const TreeletId treelet_id = *worker.treelet;
  auto& bag_queue = queued_ray_bags[treelet_id];

  /* Q1: do we have any rays to generate? */
  bool rays_to_generate = ( treelet_id == 0 ) && tiles.camera_rays_remaining();

  /* Q2: do we have any work for this worker? */
  bool work_to_do = ( bag_queue.size() > 0 );

  if ( !rays_to_generate && !work_to_do ) {
    return { true, false };
  }

  if ( ( rays_to_generate || work_to_do )
       && worker.active_rays() < WORKER_MAX_ACTIVE_RAYS ) {
    if ( rays_to_generate && !work_to_do ) {
      tiles.send_worker_tile( worker, aggregated_stats );
      rays_to_generate = tiles.camera_rays_remaining();
    } else { /* !rays_to_generate && work_to_do */
      worker.to_be_assigned.push_back( bag_queue.front() );
      record_assign( worker, bag_queue.front() );

      bag_queue.pop();
      queued_ray_bags_count--;
    }

    return { worker.active_rays() < WORKER_MAX_ACTIVE_RAYS, true };
  }

  return { worker.active_rays() < WORKER_MAX_ACTIVE_RAYS, false };
}

void LambdaMaster::handle_queued_ray_bags()
{
  GlobalScopeTimer<Timer::Category::AssigningWork> _;

  auto process_worker_list = [&]( vector<WorkerId>& workers_list ) {
    shuffle( workers_list.begin(), workers_list.end(), rand_engine );

    bool should_continue;

    do {
      should_continue = false;

      for ( const auto worker_id : workers_list ) {
        auto& worker = workers[worker_id];

        if ( not worker.marked_free )
          continue;

        auto [worker_free, work_assigned] = assign_work( worker );
        should_continue = should_continue || work_assigned;

        worker.marked_free = worker_free;
      }
    } while ( should_continue );

    for ( const auto worker_id : workers_list ) {
      auto& worker = workers[worker_id];

      if ( not worker.to_be_assigned.empty() ) {
        protobuf::RayBags proto_bags;
        for ( auto& bag_info : worker.to_be_assigned ) {
          *proto_bags.add_items() = to_protobuf( bag_info );
        }

        worker.client.push_request(
          { 0, OpCode::ProcessRayBag, protoutil::to_string( proto_bags ) } );

        worker.to_be_assigned.clear();
      }
    }
  };

  pbrt::ParallelFor(
    [&]( const uint64_t idx ) {
      if ( idx >= treelet_count ) {
        /* special case for accumulators */
        vector<WorkerId> workers_list( accumulators );
        iota( workers_list.begin(), workers_list.end(), 0 );
        process_worker_list( workers_list );
        return;
      }

      auto& treelet = treelets[idx];
      auto& treelet_queue = queued_ray_bags[idx];

      if ( treelet_queue.empty()
           and not( treelet.id == 0 and tiles.camera_rays_remaining() ) )
        return;

      vector<WorkerId> workers_list { treelet.workers.begin(),
                                      treelet.workers.end() };
      process_worker_list( workers_list );
    },
    treelet_count + ( ( accumulators > 0 ) ? 1 : 0 ),
    64 );
}

template<class T, class C>
void move_from_to( queue<T, C>& from, queue<T, C>& to )
{
  while ( !from.empty() ) {
    to.emplace( move( from.front() ) );
    from.pop();
  }
}

void LambdaMaster::move_from_pending_to_queued( const TreeletId treelet_id )
{
  queued_ray_bags_count += pending_ray_bags[treelet_id].size();
  move_from_to( pending_ray_bags[treelet_id], queued_ray_bags[treelet_id] );
}

void LambdaMaster::move_from_queued_to_pending( const TreeletId treelet_id )
{
  queued_ray_bags_count -= pending_ray_bags[treelet_id].size();
  move_from_to( queued_ray_bags[treelet_id], pending_ray_bags[treelet_id] );
}
