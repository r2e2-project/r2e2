#include <algorithm>
#include <atomic>
#include <chrono>
#include <deque>
#include <execution>
#include <iostream>
#include <map>
#include <string>
#include <thread>
#include <vector>

#include <pbrt/accelerators/cloudbvh.h>
#include <pbrt/core/geometry.h>
#include <pbrt/main.h>
#include <pbrt/raystate.h>

#include <concurrentqueue/blockingconcurrentqueue.h>
#include <concurrentqueue/concurrentqueue.h>

#include "net/address.hh"
#include "net/socket.hh"
#include "util/eventfd.hh"
#include "util/eventloop.hh"
#include "util/exception.hh"
#include "util/timerfd.hh"

using namespace std;
using namespace std::chrono;

class LocustaWorker
{
private:
  struct Peer
  {
    TCPSocket socket {};
    deque<pbrt::RayStatePtr> outgoing_rays {};

    string write_buffer {};
    string read_buffer {};

    Peer( TCPSocket&& s )
      : socket( move( s ) )
    {
    }
  };

  // handling peers
  EventLoop event_loop_ {};
  TCPSocket listen_socket_ {};
  map<int, Peer> peers_ {}; // treelet_id -> peer

  // scene data
  const uint32_t total_workers_;
  const uint32_t my_treelet_id_;
  const bool is_accumulator_ { my_treelet_id_ == total_workers_ - 1 };
  pbrt::SceneBase scene_;
  shared_ptr<pbrt::CloudBVH> treelet_;

  vector<thread> threads_ {};

  moodycamel::BlockingConcurrentQueue<pbrt::RayStatePtr> input_rays_ {};
  moodycamel::BlockingConcurrentQueue<pbrt::RayStatePtr> output_rays_ {};
  moodycamel::BlockingConcurrentQueue<pbrt::RayStatePtr> output_samples_ {};

  atomic<size_t> input_rays_size_ { 0 };
  atomic<size_t> output_rays_size_ { 0 };
  atomic<size_t> output_samples_size_ { 0 };

  atomic<size_t> total_rays_sent_ { 0 };
  atomic<size_t> total_rays_processed_ { 0 };
  atomic<size_t> total_rays_received_ { 0 };

  atomic<bool> terminated { false };

  TimerFD status_timer_ { 1s, 1s };

  void generate_rays();

  void render_thread();
  void accumulation_thread();
  void status();

public:
  LocustaWorker( const string& scene_path,
                 const uint32_t treelet_id,
                 const int spp,
                 const Address& listen_addr,
                 vector<Address>& peer_addrs )
    : total_workers_( peer_addrs.size() )
    , my_treelet_id_( treelet_id )
    , scene_( scene_path, spp )
    , treelet_( treelet_id < total_workers_ - 1 ? pbrt::LoadTreelet( scene_path, treelet_id ) : nullptr )
  {
    if ( total_workers_ != scene_.TreeletCount() + 1 ) {
      throw runtime_error( "treelet count mismatch" );
    }

    cerr << "Loaded treelet " << treelet_id << "." << endl;

    listen_socket_.bind( listen_addr );
    listen_socket_.set_blocking( false );
    listen_socket_.set_reuseaddr();
    listen_socket_.listen();

    cerr << "Listening on " << listen_addr.to_string() << "." << endl;

    {
      EventLoop setup_loop {};

      setup_loop.add_rule(
        "Listen",
        Direction::In,
        listen_socket_,
        [&] {
          auto socket = listen_socket_.accept();

          uint32_t peer_id;
          socket.set_blocking( true );

          if ( socket.read( string_view { reinterpret_cast<char*>( &peer_id ), sizeof( peer_id ) } ) != 4 ) {
            throw runtime_error( "failed to read peer id" );
          }

          socket.set_blocking( false );

          cerr << "Got a connection from " << socket.peer_address().to_string() << " (treelet " << peer_id << ")."
               << endl;

          peers_.emplace( peer_id, move( socket ) );
        },
        [] { return true; } );

      this_thread::sleep_for( 5s ); // wait for other workers to start listening

      // each worker connects to the workers after it
      for ( int i = treelet_id + 1; i < static_cast<int>( peer_addrs.size() ); i++ ) {
        TCPSocket socket;
        socket.connect( peer_addrs[i] );
        socket.write_all( { reinterpret_cast<const char*>( &treelet_id ), sizeof( treelet_id ) } );
        cerr << "Connected to " << peer_addrs[i].to_string() << " (treelet " << i << ")." << endl;
        peers_.emplace( i, move( socket ) );
      }

      while ( ( peers_.size() < total_workers_ - 1 ) and setup_loop.wait_next_event( -1 ) != EventLoop::Result::Exit ) {
      }
    }

    cerr << "Connections established to all peers." << endl;

    if ( my_treelet_id_ == 0 ) {
      generate_rays();
      cout << "All rays generated." << endl;
    }

    // now setting up the main event loop
    event_loop_.set_fd_failure_callback( [] { cerr << "FD FAILURE CALLBACK" << endl; } );

    event_loop_.add_rule(
      "Status", Direction::In, status_timer_, bind( &LocustaWorker::status, this ), [] { return true; } );

    event_loop_.add_rule(
      "Output rays",
      [&] {
        pbrt::RayStatePtr ray;
        while ( output_rays_.try_dequeue( ray ) ) {
          output_rays_size_--;
          auto peer_it = peers_.find( ray->CurrentTreelet() );
          peer_it->second.outgoing_rays.emplace_back( move( ray ) );
        }
      },
      [this] { return output_rays_size_ > 0; } );

    event_loop_.add_rule(
      "Output samples",
      [&] {
        pbrt::RayStatePtr ray;
        while ( output_samples_.try_dequeue( ray ) ) {
          output_samples_size_--;

          auto peer_it = peers_.find( total_workers_ - 1 ); // last worker is the accumulator
          peer_it->second.outgoing_rays.emplace_back( move( ray ) );
        }
      },
      [this] { return output_samples_size_ > 0; } );

    for ( auto& [peer_id, peer] : peers_ ) {
      auto peer_it = peers_.find( peer_id );
      peer.socket.set_blocking( false );

      // socket read and write events
      event_loop_.add_rule(
        "SOCKET Peer " + to_string( peer_id ),
        peer.socket,
        [peer_it] { // IN callback
          string buffer( 4096, '\0' );
          auto data_len = peer_it->second.socket.read( { buffer } );
          peer_it->second.read_buffer.append( buffer, 0, data_len );
        },
        [] { return true; },
        [peer_it] { // OUT callback
          auto data_len = peer_it->second.socket.write( string_view { peer_it->second.write_buffer } );
          peer_it->second.write_buffer.erase( 0, data_len );
        },
        [peer_it] { return not peer_it->second.write_buffer.empty(); } );

      // incoming rays event
      event_loop_.add_rule(
        "RAY-IN Peer " + to_string( peer_id ),
        [this, peer_it] {
          auto& buffer = peer_it->second.read_buffer;
          const uint32_t len = *reinterpret_cast<const uint32_t*>( buffer.data() );
          pbrt::RayStatePtr ray = pbrt::RayState::Create();
          ray->Deserialize( buffer.data() + 4, len );
          buffer.erase( 0, len + 4 );

          this->input_rays_.enqueue( move( ray ) );
          input_rays_size_++;

          total_rays_received_++;
        },
        [peer_it] {
          /* is there a full ray to read? */
          auto& buffer = peer_it->second.read_buffer;
          return not buffer.empty() and buffer.length() >= 4
                 and *reinterpret_cast<const uint32_t*>( buffer.data() ) <= buffer.length() - 4;
        } );

      // outgoing rays event
      event_loop_.add_rule(
        "RAY-OUT Peer " + to_string( peer_id ),
        [this, peer_it] {
          auto& buffer = peer_it->second.write_buffer;
          auto& rays = peer_it->second.outgoing_rays;

          while ( not rays.empty() and buffer.length() < 4096 * 1024 ) {
            auto ray = move( peer_it->second.outgoing_rays.front() );
            peer_it->second.outgoing_rays.pop_front();
            string serialized_buffer( ray->MaxCompressedSize(), '\0' );
            const uint32_t serialized_len = ray->Serialize( serialized_buffer.data() );
            buffer.append( serialized_buffer, 0, serialized_len );
            total_rays_sent_++;
          }
        },
        [peer_it] {
          return not peer_it->second.outgoing_rays.empty() and peer_it->second.write_buffer.length() < 4096 * 1024;
        } );
    }

    // let's start the threads
    if ( is_accumulator_ ) {
      threads_.emplace_back( &LocustaWorker::accumulation_thread, this );
    } else {
      threads_.emplace_back( &LocustaWorker::render_thread, this );
    }
  }

  void run()
  {
    while ( not terminated and event_loop_.wait_next_event( -1 ) != EventLoop::Result::Exit ) {
    }
  }

  ~LocustaWorker()
  {
    terminated = true;
    input_rays_.enqueue( nullptr );

    for ( auto& thread : threads_ ) {
      thread.join();
    }
  }
};

void LocustaWorker::generate_rays()
{
  for ( int sample = 0; sample < scene_.SamplesPerPixel(); sample++ ) {
    for ( pbrt::Point2i pixel : scene_.SampleBounds() ) {
      input_rays_.enqueue( scene_.GenerateCameraRay( pixel, sample ) );
      input_rays_size_++;
    }
  }
}

void LocustaWorker::render_thread()
{
  pbrt::RayStatePtr ray;
  pbrt::MemoryArena mem_arena;

  while ( not terminated ) {
    input_rays_.wait_dequeue( ray );
    if ( ray == nullptr ) {
      return;
    }

    input_rays_size_--;

    pbrt::ProcessRayOutput output;
    scene_.ProcessRay( move( ray ), *treelet_, mem_arena, output );

    total_rays_processed_++;

    for ( auto& r : output.rays ) {
      if ( r ) {
        if ( r->CurrentTreelet() == my_treelet_id_ ) {
          input_rays_.enqueue( move( r ) );
          input_rays_size_++;
        } else {
          output_rays_.enqueue( move( r ) );
          output_rays_size_++;
        }
      }
    }

    if ( output.sample ) {
      output_samples_.enqueue( move( output.sample ) );
      output_samples_size_++;
    }
  }

  cerr << "Render thread exiting." << endl;
}

void LocustaWorker::accumulation_thread()
{
  pbrt::RayStatePtr sample;
  vector<pbrt::Sample> current_samples;

  auto last_write = steady_clock::now();

  do {
    while ( input_rays_.try_dequeue( sample ) ) {
      if ( sample == nullptr ) {
        return;
      }

      input_rays_size_--;
      current_samples.emplace_back( *sample );
    }

    if ( current_samples.empty() ) {
      this_thread::sleep_for( 1s );
      continue;
    }

    scene_.AccumulateImage( current_samples );
    total_rays_processed_ += current_samples.size();

    const auto now = steady_clock::now();
    if ( now - last_write > 1s ) {
      scene_.WriteImage( "output.png" );
      last_write = now;
    }

    current_samples.clear();
  } while ( not terminated );

  cerr << "Accumulation thread exiting." << endl;
}

void LocustaWorker::status()
{
  const static auto start_time = steady_clock::now();

  status_timer_.read_event();

  const auto now = steady_clock::now();
  cerr << "(" << duration_cast<seconds>( now - start_time ).count() << "s)\t in_queue=" << input_rays_size_
       << ", out_queue=" << output_rays_size_ << ", sent=" << total_rays_sent_ << ", recv=" << total_rays_received_
       << ", proc=" << total_rays_processed_ << endl;
}

void usage( const char* argv0 )
{
  cerr << argv0 << " SCENE-DATA TREELET SPP" << endl;
}

int main( int argc, char const* argv[] )
{
  try {
    if ( argc <= 0 ) {
      abort();
    }

    if ( argc != 4 ) {
      usage( argv[0] );
      return EXIT_FAILURE;
    }

    FLAGS_log_prefix = false;
    google::InitGoogleLogging( argv[0] );

    pbrt::PbrtOptions.compressRays = false;

    vector<Address> peer_addrs {
      Address { "0.0.0.0", 45000 }, Address { "0.0.0.0", 45001 }, Address { "0.0.0.0", 45002 },
      Address { "0.0.0.0", 45003 }, Address { "0.0.0.0", 45004 }, Address { "0.0.0.0", 45005 },
    };

    const string scene_path { argv[1] };
    const uint32_t treelet_id { static_cast<uint32_t>( stoi( argv[2] ) ) };
    const int spp { stoi( argv[3] ) };

    LocustaWorker worker { scene_path, treelet_id, spp, peer_addrs[treelet_id], peer_addrs };
    worker.run();

  } catch ( exception& ex ) {
    print_exception( argv[0], ex );
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}
