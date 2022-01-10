#include "lambda-worker.hh"
#include "messages/utils.hh"
#include "net/util.hh"

#include "scene.h"

using namespace std;
using namespace chrono;
using namespace r2t2;
using namespace pbrt;
using namespace meow;

using OpCode = Message::OpCode;

void LambdaWorker::generate_rays( const Bounds2i& bounds )
{
  /* for ray tracking */
  bernoulli_distribution bd { config.ray_log_rate };

  for ( int sample = 0; sample < scene.base.samplesPerPixel; sample++ ) {
    for ( const Point2i pixel : bounds ) {
      RayStatePtr state_ptr
        = graphics::GenerateCameraRay( scene.base.camera,
                                       pixel,
                                       sample,
                                       scene.max_depth,
                                       scene.base.sampleExtent,
                                       scene.base.sampler );

      state_ptr->trackRay = track_rays ? bd( rand_engine ) : false;
      const TreeletId next_treelet = state_ptr->CurrentTreelet();

      if ( treelets.count( next_treelet ) ) {
        trace_queue_size++;
        trace_queue.enqueue( move( state_ptr ) );
      } else {
        out_queue[next_treelet].push( move( state_ptr ) );
        out_queue_size++;
      }
    }
  }
}

void LambdaWorker::handle_trace_queue( const size_t idx )
{
  pbrt::RayStatePtr ray_ptr;

  constexpr size_t RAYS_TO_NOTIFY = 1000;
  size_t ray_counter = 0;

  // assuming only one treelet per worker
  auto& treelet = *treelets.begin()->second;

  while ( true ) {
    trace_queue.wait_dequeue( ray_ptr );

    /* XXX maybe we need to revisit this decision */
    MemoryArena arena;

    do {
      trace_queue_size--;

      if ( ray_counter++ < RAYS_TO_NOTIFY ) {
        rays_ready_fd.write_event();
      }

      if ( ray_ptr == nullptr ) {
        raytracing_thread_stats[idx] = pbrt::stats::GetThreadStats();
        return;
      }

      pbrt::graphics::ProcessRayOutput out;
      graphics::ProcessRay( move( ray_ptr ), treelet, scene.base, arena, out );
      processed_queue_size++;
      processed_queue.enqueue( move( out ) );
    } while ( trace_queue.try_dequeue( ray_ptr ) );

    rays_ready_fd.write_event();
  }
}

void LambdaWorker::handle_processed_queue()
{
  if ( not rays_ready_fd.read_event() ) {
    return;
  }

  auto queue_ray = [this]( pbrt::RayStatePtr&& ray ) {
    const TreeletId next_treelet = ray->CurrentTreelet();

    if ( treelets.count( next_treelet ) ) {
      trace_queue_size++;
      trace_queue.enqueue( move( ray ) );
    } else {
      out_queue[next_treelet].push( move( ray ) );
      out_queue_size++;
    }

    ray_counters.generated++;
  };

  auto queue_sample = [this]( pbrt::RayStatePtr&& ray ) {
    samples.emplace( *ray );
    ray_counters.generated++;
  };

  pbrt::graphics::ProcessRayOutput output;
  while ( processed_queue.try_dequeue( output ) ) {
    processed_queue_size--;

    for ( auto& ray : output.rays ) {
      if ( ray ) {
        queue_ray( move( ray ) );
      }
    }

    if ( output.sample ) {
      queue_sample( move( output.sample ) );
    }

    if ( output.pathFinished ) {
      finished_path_ids.push( output.pathId );
    }
  }
}
