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

  for ( size_t sample = 0; sample < scene.base.samplesPerPixel; sample++ ) {
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
        trace_queue[next_treelet].push( move( state_ptr ) );
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
  queue<RayStatePtr> processed_rays;

  // constexpr size_t MAX_RAYS = WORKER_MAX_ACTIVE_RAYS / 2;
  const auto trace_until = steady_clock::now() + 100ms;
  size_t traced_count = 0;
  MemoryArena arena;

  while ( !trace_queue.empty() && steady_clock::now() <= trace_until ) {
    for ( auto const& [treelet_id, treelet_ptr] : treelets ) {
      const CloudBVH& treelet = *treelet_ptr;

      auto rays_it = trace_queue.find( treelet_id );
      if ( rays_it == trace_queue.end() )
        continue;

      auto& rays = rays_it->second;

      while ( !rays.empty() && steady_clock::now() <= trace_until ) {
        traced_count++;
        RayStatePtr ray_ptr = move( rays.front() );
        rays.pop();

        this->rays.terminated++;

        RayState& ray = *ray_ptr;
        const uint64_t path_id = ray.PathID();

        log_ray( RayAction::Traced, ray );

        if ( !ray.toVisitEmpty() ) {
          const uint32_t ray_treelet = ray.toVisitTop().treelet;
          auto new_ray_ptr = graphics::TraceRay( move( ray_ptr ), treelet );
          auto& new_ray = *new_ray_ptr;

          const bool hit = new_ray.hit;
          const bool empty_visit = new_ray.toVisitEmpty();

          if ( new_ray.isShadowRay ) {
            if ( hit || empty_visit ) {
              new_ray.Ld = hit ? 0.f : new_ray.Ld;
              local_stats.shadow_ray_hops.add( new_ray.hop );
              samples.emplace( *new_ray_ptr );
              this->rays.generated++;

              log_ray( RayAction::Finished, new_ray );

              /* was this the last shadow ray? */
              if ( new_ray.remainingBounces == 0 ) {
                finished_path_ids.push( path_id );
                local_stats.path_hops.add( new_ray.pathHop );
              }
            } else {
              processed_rays.push( move( new_ray_ptr ) );
            }
          } else if ( !empty_visit || hit ) {
            processed_rays.push( move( new_ray_ptr ) );
          } else if ( empty_visit ) {
            new_ray.Ld = 0.f;
            samples.emplace( *new_ray_ptr );
            finished_path_ids.push( path_id );
            this->rays.generated++;

            local_stats.ray_hops.add( new_ray.hop );
            local_stats.path_hops.add( new_ray.pathHop );

            log_ray( RayAction::Finished, new_ray );
          }
        } else if ( ray.hit ) {
          local_stats.ray_hops.add( ray.hop );
          log_ray( RayAction::Finished, ray );

          RayStatePtr bounce_ray, shadow_ray;
          tie( bounce_ray, shadow_ray )
            = graphics::ShadeRay( move( ray_ptr ),
                                  treelet,
                                  scene.base.lights,
                                  scene.base.sampleExtent,
                                  scene.base.sampler,
                                  scene.max_depth,
                                  arena );

          if ( bounce_ray == nullptr && shadow_ray == nullptr ) {
            /* this was the last ray in the path */
            finished_path_ids.push( path_id );
            local_stats.path_hops.add( ray.pathHop );
          }

          if ( bounce_ray != nullptr ) {
            log_ray( RayAction::Generated, *bounce_ray );
            processed_rays.push( move( bounce_ray ) );
          }

          if ( shadow_ray != nullptr ) {
            log_ray( RayAction::Generated, *shadow_ray );
            processed_rays.push( move( shadow_ray ) );
          }
        } else {
          throw runtime_error( "invalid ray in ray queue" );
        }
      }

      if ( rays.empty() ) {
        trace_queue.erase( rays_it );
      }
    }
  }

  while ( !processed_rays.empty() ) {
    RayStatePtr ray = move( processed_rays.front() );
    processed_rays.pop();

    const TreeletId next_treelet = ray->CurrentTreelet();

    if ( treelets.count( next_treelet ) ) {
      trace_queue[next_treelet].push( move( ray ) );
    } else {
      log_ray( RayAction::Queued, *ray );
      out_queue[next_treelet].push( move( ray ) );
      out_queue_size++;
    }

    this->rays.generated++;
  }
}
