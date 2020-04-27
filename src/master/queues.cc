#include "lambda-master.hh"
#include "messages/utils.hh"
#include "util/random.hh"

using namespace std;
using namespace r2t2;
using namespace meow;

using OpCode = Message::OpCode;

pair<bool, bool> LambdaMaster::assign_work( Worker& worker )
{
  /* return, if worker is not active anymore */
  if ( worker.state != Worker::State::Active )
    return { false, false };

  /* return if the worker already has enough work */
  if ( worker.active_rays() >= WORKER_MAX_ACTIVE_RAYS )
    return { false, false };

  /* return, if the worker doesn't have any treelets */
  if ( worker.treelets.empty() )
    return { false, false };

  const TreeletId treelet_id = worker.treelets[0];
  auto& bag_queue = queued_ray_bags[treelet_id];

  /* Q1: do we have any rays to generate? */
  bool rays_to_generate = tiles.camera_rays_remaining() && ( treelet_id == 0 );

  /* Q2: do we have any work for this worker? */
  bool work_to_do = ( bag_queue.size() > 0 );

  if ( !rays_to_generate && !work_to_do ) {
    return { true, false };
  }

  if ( ( rays_to_generate || work_to_do )
       && worker.active_rays() < WORKER_MAX_ACTIVE_RAYS ) {
    if ( rays_to_generate && !work_to_do ) {
      tiles.send_worker_tile( worker );
      rays_to_generate = tiles.camera_rays_remaining();
    } else {
      /* only if work_to_do or the coin flip returned false */
      *worker.to_be_assigned.add_items() = to_protobuf( bag_queue.front() );
      record_assign( worker.id, bag_queue.front() );

      bag_queue.pop();
      queued_ray_bags_count--;
    }

    return { worker.active_rays() < WORKER_MAX_ACTIVE_RAYS, true };
  }

  return { worker.active_rays() < WORKER_MAX_ACTIVE_RAYS, false };
}

void LambdaMaster::handle_queued_ray_bags()
{
  shuffle( free_workers.begin(), free_workers.end(), rand_engine );
  bool should_continue;

  do {
    should_continue = false;

    for ( const WorkerId worker_id : free_workers ) {
      auto& worker = workers[worker_id];

      auto [worker_free, work_assigned] = assign_work( worker );
      should_continue = should_continue || work_assigned;

      worker.marked_free = worker_free;
    }
  } while ( should_continue );

  vector<WorkerId> new_free_workers;
  new_free_workers.reserve( free_workers.size() );

  for ( auto& worker : workers ) {
    if ( worker.to_be_assigned.items_size() ) {
      worker.client.push_request(
        { 0,
          OpCode::ProcessRayBag,
          protoutil::to_string( worker.to_be_assigned ) } );

      worker.to_be_assigned = {};
    }

    if ( worker.marked_free ) {
      worker.marked_free = false;
      new_free_workers.push_back( worker.id );
    }
  }

  swap( free_workers, new_free_workers );
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
