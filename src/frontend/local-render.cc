#include <algorithm>
#include <atomic>
#include <chrono>
#include <execution>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include <pbrt/accelerators/cloudbvh.h>
#include <pbrt/core/geometry.h>
#include <pbrt/main.h>
#include <pbrt/raystate.h>

#include <concurrentqueue/concurrentqueue.h>

#include "util/exception.hh"

using namespace std;
using namespace std::chrono;

class LocalRenderer
{
private:
  pbrt::SceneBase scene_;
  vector<shared_ptr<pbrt::CloudBVH>> treelets_ {};

  vector<thread> threads_ {};
  moodycamel::ConcurrentQueue<pbrt::RayStatePtr> rays_ {};
  moodycamel::ConcurrentQueue<pbrt::RayStatePtr> samples_ {};

  atomic<bool> all_rays_generated_ { false };

  steady_clock::time_point start_time_ { steady_clock::now() };

  void render_worker();
  void accumulation_worker();
  void status();

public:
  LocalRenderer( const string& scene_path,
                 const int spp,
                 const size_t thread_count = thread::hardware_concurrency() )
    : scene_( scene_path, spp )
  {
    if ( thread_count < 2 ) {
      throw runtime_error( "at least 5 rendering threads are needed" );
    }

    treelets_.resize( scene_.TreeletCount() );

    vector<size_t> treelet_idx;
    treelet_idx.resize( scene_.TreeletCount() );
    iota( treelet_idx.begin(), treelet_idx.end(), 0 );

    const auto start_time = steady_clock::now();

    cerr << "Loading " << scene_.TreeletCount() << " treelets... ";

    transform(
      execution::par_unseq,
      treelet_idx.begin(),
      treelet_idx.end(),
      treelets_.begin(),
      [&]( const auto tid ) { return pbrt::LoadTreelet( scene_path, tid ); } );

    const auto end_time = steady_clock::now();

    cerr << "done (" << duration_cast<seconds>( end_time - start_time ).count()
         << "s)." << endl;

    // start the status thread
    threads_.emplace_back( &LocalRenderer::status, this );

    // start rendering threads
    cerr << "Starting " << ( thread_count - 4 ) << " rendering thread(s)... ";
    for ( size_t i = 0; i < thread_count - 4; i++ ) {
      threads_.emplace_back( &LocalRenderer::render_worker, this );
    }
    cerr << "done." << endl;

    // start accumulation thread
    cerr << "Starting " << 4 << " accumulation thread(s)... ";
    for ( size_t i = 0; i < 4; i++ ) {
      threads_.emplace_back( &LocalRenderer::accumulation_worker, this );
    }
    cerr << "done." << endl;

    // generate all the camera rays
    for ( int sample = 0; sample < scene_.SamplesPerPixel(); sample++ ) {
      for ( pbrt::Point2i pixel : scene_.SampleBounds() ) {
        rays_.enqueue( scene_.GenerateCameraRay( pixel, sample ) );
      }
    }

    all_rays_generated_ = true;
  }

  ~LocalRenderer()
  {
    for ( auto& thread : threads_ ) {
      thread.join();
    }

    scene_.WriteImage( "output.png" );
  }
};

void LocalRenderer::render_worker()
{
  pbrt::RayStatePtr ray;
  pbrt::MemoryArena mem_arena;

  while ( true ) {
    if ( not rays_.try_dequeue( ray ) ) {
      if ( all_rays_generated_ ) {
        return;
      } else {
        this_thread::sleep_for( 500ms );
        continue;
      }
    }

    pbrt::ProcessRayOutput output;
    auto& treelet = *treelets_[ray->CurrentTreelet()];
    scene_.ProcessRay( move( ray ), treelet, mem_arena, output );

    for ( auto& r : output.rays )
      if ( r )
        rays_.enqueue( move( r ) );

    if ( output.sample )
      samples_.enqueue( move( output.sample ) );
  }
}

void LocalRenderer::accumulation_worker()
{
  pbrt::RayStatePtr sample;
  vector<pbrt::Sample> current_samples;

  do {
    while ( samples_.try_dequeue( sample ) ) {
      current_samples.emplace_back( *sample );
    }

    if ( current_samples.empty() ) {
      this_thread::sleep_for( 1s );
      continue;
    }

    scene_.AccumulateImage( current_samples );
    current_samples.clear();
  } while ( rays_.size_approx() > 0 or samples_.size_approx() > 0
            or not all_rays_generated_ );
}

void LocalRenderer::status()
{
  while ( rays_.size_approx() > 0 or samples_.size_approx() > 0
          or not all_rays_generated_ ) {
    this_thread::sleep_for( 1s );
    const auto now = steady_clock::now();
    cerr << "(" << duration_cast<seconds>( now - start_time_ ).count()
         << "s)\trays=" << rays_.size_approx()
         << ", samples=" << samples_.size_approx() << endl;
  }
}

void usage( const char* argv0 )
{
  cerr << argv0 << " SCENE-DATA SPP" << endl;
}

int main( int argc, char const* argv[] )
{
  try {
    if ( argc <= 0 ) {
      abort();
    }

    if ( argc != 3 ) {
      usage( argv[0] );
      return EXIT_FAILURE;
    }

    FLAGS_log_prefix = false;
    google::InitGoogleLogging( argv[0] );

    const string scene_path { argv[1] };
    const int spp { stoi( argv[2] ) };

    LocalRenderer renderer { scene_path, spp, thread::hardware_concurrency() };
  } catch ( exception& ex ) {
    print_exception( argv[0], ex );
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}
