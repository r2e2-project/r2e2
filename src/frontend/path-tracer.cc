#include <getopt.h>
#include <iostream>
#include <memory>
#include <queue>
#include <string>

#include <pbrt/accelerators/cloudbvh.h>
#include <pbrt/core/geometry.h>
#include <pbrt/main.h>
#include <pbrt/raystate.h>

using namespace std;

/* This is a simple ray tracer, built using the api provided by r2t2's fork of
pbrt. */

void usage( char* argv0 )
{
  cerr << argv0 << " SCENE-PATH [SAMPLES-PER-PIXEL]" << endl;
}

int main( int argc, char* argv[] )
{
  if ( argc <= 0 ) {
    abort();
  }

  if ( argc < 2 ) {
    usage( argv[0] );
    return EXIT_FAILURE;
  }

  const size_t max_depth = 5;
  const string scene_path { argv[1] };
  const size_t samples_per_pixel = ( argc > 2 ) ? stoull( argv[2] ) : 0;

  /* (1) loading the scene */
  auto scene_base = pbrt::scene::LoadBase( scene_path, samples_per_pixel );

  /* (2) loading all the treelets */
  vector<shared_ptr<pbrt::CloudBVH>> treelets;

  for ( size_t i = 0; i < scene_base.GetTreeletCount(); i++ ) {
    treelets.push_back( pbrt::scene::LoadTreelet( scene_path, i ) );
  }

  /* (3) generating all the initial rays */
  queue<pbrt::RayStatePtr> ray_queue;

  for ( const auto pixel : scene_base.sampleBounds ) {
    for ( int sample = 0; sample < scene_base.samplesPerPixel; sample++ ) {
      ray_queue.push(
        pbrt::graphics::GenerateCameraRay( scene_base.camera,
                                           pixel,
                                           sample,
                                           max_depth,
                                           scene_base.sampleExtent,
                                           scene_base.sampler ) );
    }
  }

  /* (4) tracing rays to completing */
  vector<pbrt::Sample> samples;

  pbrt::MemoryArena arena;

  while ( not ray_queue.empty() ) {
    pbrt::RayStatePtr ray = move( ray_queue.front() );
    ray_queue.pop();

    auto& treelet = treelets[ray->CurrentTreelet()];

    /* This is the ray tracing core logic; calling one of TraceRay or ShadeRay
    functions, based on the state of the `ray`, and putting the result back
    into the ray_queue for further processing, or into samples when they are
    done. */
    if ( not ray->toVisitEmpty() ) {
      auto new_ray = pbrt::graphics::TraceRay( move( ray ), *treelet );

      const bool hit = new_ray->HasHit();
      const bool empty_visit = new_ray->toVisitEmpty();

      if ( new_ray->IsShadowRay() ) {
        if ( hit or empty_visit ) {
          new_ray->Ld = hit ? 0.f : new_ray->Ld;
          samples.emplace_back( *new_ray ); // this ray is done
        } else {
          ray_queue.push( move( new_ray ) ); // back to the ray queue
        }
      } else if ( not empty_visit or hit ) {
        ray_queue.push( move( new_ray ) ); // back to the ray queue
      } else if ( empty_visit ) {
        new_ray->Ld = 0.f;
        samples.emplace_back( *new_ray ); // this ray is done
      }
    } else if ( ray->HasHit() ) {
      auto [bounce_ray, shadow_ray]
        = pbrt::graphics::ShadeRay( move( ray ),
                                    *treelet,
                                    scene_base.lights,
                                    scene_base.sampleExtent,
                                    scene_base.sampler,
                                    max_depth,
                                    arena );

      if ( bounce_ray != nullptr ) {
        ray_queue.push( move( bounce_ray ) ); // back to the ray queue
      }

      if ( shadow_ray != nullptr ) {
        ray_queue.push( move( shadow_ray ) ); // back to the ray queue
      }
    }
  }

  /* (5) accumulating the samples and producing the final output */
  pbrt::graphics::AccumulateImage( scene_base.camera, samples );
  pbrt::graphics::WriteImage( scene_base.camera );

  return EXIT_SUCCESS;
}
