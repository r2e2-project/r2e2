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
      log_ray( RayAction::Generated, *state_ptr );

      const TreeletId next_treelet = state_ptr->CurrentTreelet();

      if ( treelets.count( next_treelet ) ) {
        trace_queue_size++;
        trace_queue.enqueue( move( state_ptr ) );
      } else {
        log_ray( RayAction::Queued, *state_ptr );
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

      auto& ray = *ray_ptr;

      log_ray( RayAction::Traced, ray );
      ray_counters.terminated++;

      if ( not ray.toVisitEmpty() ) {
        processed_queue_size++;
        processed_queue.enqueue(
          graphics::TraceRay( move( ray_ptr ), treelet ) );
      } else if ( ray.hit ) {
        log_ray( RayAction::Finished, ray );

        auto [bounce_ray, shadow_ray, light_ray]
          = graphics::ShadeRay( move( ray_ptr ),
                                treelet,
                                *scene.base.fakeScene,
                                scene.base.sampleExtent,
                                scene.base.sampler,
                                scene.max_depth,
                                arena );

        if ( bounce_ray == nullptr and shadow_ray == nullptr ) {
          // means that the path was terminated
          ray.toVisitHead = numeric_limits<uint8_t>::max();
          processed_queue_size++;
          processed_queue.enqueue( move( ray_ptr ) );
          continue;
        }

        if ( bounce_ray != nullptr ) {
          log_ray( RayAction::Generated, *bounce_ray );
          processed_queue_size++;
          processed_queue.enqueue( move( bounce_ray ) );
        }

        if ( shadow_ray != nullptr ) {
          log_ray( RayAction::Generated, *shadow_ray );
          processed_queue_size++;
          processed_queue.enqueue( move( shadow_ray ) );
        }

        if ( light_ray != nullptr ) {
          log_ray( RayAction::Generated, *light_ray );
          processed_queue_size++;
          processed_queue.enqueue( move( light_ray ) );
        }
      } else {
        throw runtime_error( "invalid ray in trace queue" );
      }
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
      log_ray( RayAction::Queued, *ray );
      out_queue[next_treelet].push( move( ray ) );
      out_queue_size++;
    }

    ray_counters.generated++;
  };

  pbrt::RayStatePtr ray_ptr;

  while ( processed_queue.try_dequeue( ray_ptr ) ) {
    processed_queue_size--;

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
        ray_counters.generated++;

        log_ray( RayAction::Finished, ray );

        /* was this the last shadow ray? */
        if ( ray.remainingBounces == 0 ) {
          finished_path_ids.push( ray.PathID() );
        }
      } else {
        queue_ray( move( ray_ptr ) );
      }
    } else if ( ray.IsLightRay() ) {
      Spectrum Li { 0.f };

      if ( empty_visit and hit ) {
        const auto a_light = ray.hitInfo.arealight;
        const auto s_light = ray.lightRayInfo.sampledLightId;

        if ( a_light == s_light ) {
          Li = dynamic_cast<AreaLight*>(
                 scene.base.fakeScene->lights[a_light - 1].get() )
                 ->L( ray.hitInfo.isect, -ray.lightRayInfo.sampledDirection );
        }
      } else if ( empty_visit ) {
        const auto s_light = ray.lightRayInfo.sampledLightId;
        Li = scene.base.fakeScene->lights[s_light - 1]->Le( ray.ray );
      } else {
        queue_ray( move( ray_ptr ) );
      }

      if ( empty_visit and not Li.IsBlack() ) {
        ray.Ld *= Li;
        samples.emplace( ray );
        ray_counters.generated++;
        log_ray( RayAction::Finished, ray );
      }
    } else if ( not empty_visit or hit ) {
      queue_ray( move( ray_ptr ) );
    } else if ( empty_visit ) {
      ray.Ld = 0.f;

      // XXX Ideally, this must not be done in the program logic and must be
      // hidden behind the API, maybe in the Shade() function... However, at the
      // time this was the quickest way to get it done.
      if ( ray.remainingBounces == config.max_path_depth - 1 ) {
        for ( const auto& light : scene.base.fakeScene->infiniteLights ) {
          ray.Ld += light->Le( ray.ray );
        }
      }

      samples.emplace( ray );
      finished_path_ids.push( ray.PathID() );
      ray_counters.generated++;

      log_ray( RayAction::Finished, ray );
    }
  }
}
