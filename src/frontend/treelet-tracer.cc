#include <fstream>
#include <iostream>
#include <lz4.h>
#include <map>
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
  cerr << argv0 << " SCENE-PATH TREELET-ID" << endl;
}

string open_and_decompress_bag( const string& path )
{
  ifstream fin { path };
  ostringstream buffer;
  buffer << fin.rdbuf();
  const string data = buffer.str();

  constexpr size_t max_bag_size = 4 * 1024 * 1024;
  char out[max_bag_size];
  int L = LZ4_decompress_safe( data.data(), out, data.size(), max_bag_size );

  if ( L < 0 ) {
    throw runtime_error( "bag decompression failed: " + path );
  }

  return { out, static_cast<size_t>( L ) };
}

void trace_ray( pbrt::scene::Base& scene_base,
                shared_ptr<pbrt::CloudBVH>& treelet,
                pbrt::RayStatePtr&& ray,
                map<pbrt::TreeletId, size_t>& out_count,
                size_t& sample_count,
                pbrt::MemoryArena& arena )
{
  if ( not ray->toVisitEmpty() ) {
    auto new_ray = pbrt::graphics::TraceRay( move( ray ), *treelet );

    const bool hit = new_ray->HasHit();
    const bool empty_visit = new_ray->toVisitEmpty();

    if ( new_ray->IsShadowRay() ) {
      if ( hit or empty_visit ) {
        sample_count++;
      } else {
        out_count[new_ray->CurrentTreelet()]++;
      }
    } else if ( not empty_visit or hit ) {
      out_count[new_ray->CurrentTreelet()]++;
    } else if ( empty_visit ) {
      sample_count++;
    }
  } else if ( ray->HasHit() ) {
    auto [bounce_ray, shadow_ray]
      = pbrt::graphics::ShadeRay( move( ray ),
                                  *treelet,
                                  scene_base.lights,
                                  scene_base.sampleExtent,
                                  scene_base.sampler,
                                  5,
                                  arena );

    if ( bounce_ray != nullptr ) {
      out_count[bounce_ray->CurrentTreelet()]++;
    }

    if ( shadow_ray != nullptr ) {
      out_count[shadow_ray->CurrentTreelet()]++;
    }
  }
}

int main( int argc, char* argv[] )
{
  if ( argc <= 0 ) {
    abort();
  }

  if ( argc != 3 ) {
    usage( argv[0] );
    return EXIT_FAILURE;
  }

  const string scene_path { argv[1] };
  const size_t treelet_id = stoull( argv[2] );

  /* (1) loading the scene */
  auto scene_base = pbrt::scene::LoadBase( scene_path, 0 );

  /* (2) loading the treelet */
  auto treelet = pbrt::scene::LoadTreelet( scene_path, treelet_id );

  cerr << "Treelet " << treelet_id << " loaded." << endl;

  size_t processed_bags = 0;
  size_t processed_rays = 0;
  map<pbrt::TreeletId, size_t> out_count;
  size_t sample_count = 0;

  pbrt::MemoryArena arena;

  for ( string line; getline( cin, line ); ) {
    const string bag = open_and_decompress_bag( line );
    const char* data = bag.data();

    if ( processed_bags % 100 == 0 ) {
      cerr << processed_bags << "...";
    }

    processed_bags++;

    for ( size_t offset = 0; offset < bag.size(); ) {
      uint32_t len;
      memcpy( &len, data + offset, sizeof( uint32_t ) );
      offset += 4;

      pbrt::RayStatePtr ray = pbrt::RayState::Create();
      ray->Deserialize( data + offset, len );
      offset += len;

      processed_rays++;
      trace_ray(
        scene_base, treelet, move( ray ), out_count, sample_count, arena );
    }
  }

  cout << endl;
  cout << "processed_bags = " << processed_bags << endl;
  cout << "processed_rays = " << processed_rays << endl;
  cout << "samples = " << sample_count << endl;

  for ( auto& [tid, count] : out_count ) {
    cout << "T" << tid << " = " << count << endl;
  }

  return EXIT_SUCCESS;
}
