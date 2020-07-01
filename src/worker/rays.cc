#include "lambda-worker.hh"
#include "messages/utils.hh"
#include "net/util.hh"

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
  trace_queue_empty = false;

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
      log_ray( RayAction::Generated, *state_ptr );

      const TreeletId next_treelet = state_ptr->CurrentTreelet();

      if ( treelets.count( next_treelet ) ) {
        trace_queue.enqueue( move( state_ptr ) );
      } else {
        log_ray( RayAction::Queued, *state_ptr );
        out_queue[next_treelet].push( move( state_ptr ) );
        out_queue_size++;
      }
    }
  }
}

void LambdaWorker::handle_trace_queue()
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
      if ( ray_counter++ < RAYS_TO_NOTIFY ) {
        processed_queue_empty = false;
        rays_ready_fd.write_event();
      }

      if ( ray_ptr == nullptr ) {
        return;
      }

      auto& ray = *ray_ptr;

      log_ray( RayAction::Traced, ray );
      this->rays.terminated++;

      if ( not ray.toVisitEmpty() ) {
        processed_queue.enqueue(
          graphics::TraceRay( move( ray_ptr ), treelet ) );
      } else if ( ray.hit ) {
        log_ray( RayAction::Finished, ray );

        auto [bounce_ray, shadow_ray]
          = graphics::ShadeRay( move( ray_ptr ),
                                treelet,
                                scene.base.lights,
                                scene.base.sampleExtent,
                                scene.base.sampler,
                                scene.max_depth,
                                arena );

        if ( bounce_ray == nullptr and shadow_ray == nullptr ) {
          // means that the path was terminated
          ray.toVisitHead = numeric_limits<uint8_t>::max();
          processed_queue.enqueue( move( ray_ptr ) );
          continue;
        }

        if ( bounce_ray != nullptr ) {
          log_ray( RayAction::Generated, *bounce_ray );
          processed_queue.enqueue( move( bounce_ray ) );
        }

        if ( shadow_ray != nullptr ) {
          log_ray( RayAction::Generated, *shadow_ray );
          processed_queue.enqueue( move( shadow_ray ) );
        }
      } else {
        throw runtime_error( "invalid ray in trace queue" );
      }
    } while ( trace_queue.try_dequeue( ray_ptr ) );

    processed_queue_empty = false;
    trace_queue_empty = true;
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
      trace_queue.enqueue( move( ray ) );
    } else {
      log_ray( RayAction::Queued, *ray );
      out_queue[next_treelet].push( move( ray ) );
      out_queue_size++;
    }

    this->rays.generated++;
  };

  pbrt::RayStatePtr ray_ptr;

  while ( processed_queue.try_dequeue( ray_ptr ) ) {
    auto& ray = *ray_ptr;

    /* HACK */
    if ( ray.toVisitHead == numeric_limits<uint8_t>::max() ) {
      finished_path_ids.push( ray.PathID() );
      continue;
    }

    const bool hit = ray.HasHit();
    const bool empty_visit = ray.toVisitEmpty();

    if ( ray.IsShadowRay() ) {
      if ( hit or empty_visit ) {
        ray.Ld = hit ? 0.f : ray.Ld;
        samples.emplace( ray );
        this->rays.generated++;

        log_ray( RayAction::Finished, ray );

        /* was this the last shadow ray? */
        if ( ray.remainingBounces == 0 ) {
          finished_path_ids.push( ray.PathID() );
        }
      } else {
        queue_ray( move( ray_ptr ) );
      }
    } else if ( not empty_visit or hit ) {
      queue_ray( move( ray_ptr ) );
    } else if ( empty_visit ) {
      ray.Ld = 0.f;
      samples.emplace( ray );
      finished_path_ids.push( ray.PathID() );
      this->rays.generated++;

      log_ray( RayAction::Finished, ray );
    }
  }

  processed_queue_empty = true;
}
