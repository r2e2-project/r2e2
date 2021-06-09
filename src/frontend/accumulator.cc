#include "accumulator/accumulator.hh"

#include <chrono>
#include <fstream>
#include <sstream>

#include <pbrt/core/camera.h>

#include "messages/utils.hh"

using namespace std;
using namespace r2t2;
using namespace meow;

using OpCode = Message::OpCode;

void usage( const char* argv0 )
{
  cerr << "Usage: " << argv0 << " PORT" << endl;
}

Accumulator::Accumulator( const uint16_t listen_port )
{
  filesystem::current_path( working_directory_.name() );

  listener_socket_.set_blocking( false );
  listener_socket_.set_reuseaddr();
  listener_socket_.bind( { "0.0.0.0", listen_port } );
  listener_socket_.listen();

  loop_.add_rule(
    "Listener",
    Direction::In,
    listener_socket_,
    [this] {
      TCPSocket socket = listener_socket_.accept();
      cerr << "Connection accepted from " << socket.peer_address().to_string()
           << endl;

      workers_.emplace_back( move( socket ) );
      auto worker_it = std::prev( workers_.end() );

      workers_.back().install_rules(
        loop_,
        rule_categories,
        [this, worker_it]( Message&& msg ) {
          process_message( worker_it, move( msg ) );
        },
        [this, worker_it] { workers_.erase( worker_it ); } );
    },
    [] { return true; } );

  loop_.add_rule(
    "Upload output",
    Direction::In,
    upload_output_timer_,
    [this] {
      upload_output_timer_.read_event();

      if ( not dirty_ )
        return;

      pbrt::graphics::WriteImage( scene_.camera, output_filename_ );
      ifstream fin { output_filename_ };
      stringstream buffer;
      buffer << fin.rdbuf();

      job_transfer_agent_->request_upload( output_key_, buffer.str() );
      dirty_ = false;
    },
    [] { return true; } );
}

Accumulator::~Accumulator()
{
  try {
    bags_queue.enqueue( ""s );
    handle_samples_thread_.join();

    if ( not working_directory_.name().empty() ) {
      filesystem::remove_all( working_directory_.name() );
    }
  } catch ( exception& ex ) {
  }
}

void Accumulator::process_message(
  list<meow::Client<TCPSession>>::iterator worker_it,
  Message&& msg )
{
  switch ( msg.opcode() ) {
    case OpCode::SetupAccumulator: {
      protobuf::SetupAccumulator proto;
      protoutil::from_string( msg.payload(), proto );

      job_id_ = proto.job_id();

      Storage backend_info { proto.storage_backend() };
      S3StorageBackend scene_backend {
        {}, backend_info.bucket, backend_info.region, backend_info.path
      };

      job_transfer_agent_ = make_unique<S3TransferAgent>(
        S3StorageBackend { {}, backend_info.bucket, backend_info.region },
        1,
        true );

      loop_.add_rule(
        "Uploaded",
        Direction::In,
        job_transfer_agent_->eventfd(),
        [this] {
          if ( not job_transfer_agent_->eventfd().read_event() )
            return;

          vector<pair<uint64_t, string>> actions;
          job_transfer_agent_->try_pop_bulk( back_inserter( actions ) );

          for ( auto& action : actions ) {
            cerr << "Upload done (" << action.first << ")." << endl;
          }
        },
        [] { return true; } );

      dimensions_ = make_pair( proto.width(), proto.height() );
      tile_count_ = proto.tile_count();
      tile_id_ = proto.tile_id();

      output_filename_ = to_string( tile_id_ ) + ".png";
      output_key_ = "jobs/" + job_id_ + "/output/" + output_filename_;

      // (1) download scene objects & load the scene
      vector<storage::GetRequest> get_requests;

      for ( auto& obj_proto : proto.scene_objects() ) {
        auto obj = from_protobuf( obj_proto );
        get_requests.emplace_back(
          ( obj.alt_name.empty()
              ? pbrt::scene::GetObjectName( obj.key.type, obj.key.id )
              : obj.alt_name ),
          pbrt::scene::GetObjectName( obj.key.type, obj.key.id ) );
      }

      cerr << "Downloading scene objects... ";
      scene_backend.get( get_requests );
      cerr << "done." << endl;

      scene_ = { working_directory_.name(), 1 };

      tile_helper_ = { tile_count_,
                       scene_.sampleBounds,
                       static_cast<uint32_t>( scene_.samplesPerPixel ) };

      scene_.camera->film->SetCroppedPixelBounds(
        static_cast<pbrt::Bounds2i>( tile_helper_.bounds( tile_id_ ) ) );

      cerr << "Starting worker thread... ";
      if ( handle_samples_thread_.joinable() ) {
        bags_queue.enqueue( ""s );
        handle_samples_thread_.join();
      }

      handle_samples_thread_ = thread( &Accumulator::handle_bags_queue, this );
      cerr << "done." << endl;

      worker_it->push_request(
        { 0, OpCode::SetupAccumulator, to_string( tile_id_ ) } );

      break;
    }

    case OpCode::ProcessSampleBag: {
      bags_queue.enqueue( move( msg.payload() ) );
      worker_it->push_request( { 0, OpCode::SampleBagProcessed, "" } );
      dirty_ = true;

      break;
    }

    default:
      throw runtime_error( "unexcepted opcode" );
  }
}

void Accumulator::handle_bags_queue()
{
  string sample_bag;

  while ( true ) {
    bags_queue.wait_dequeue( sample_bag );

    do {
      if ( sample_bag.empty() ) {
        return;
      }

      vector<pbrt::Sample> samples;

      for ( size_t offset = 0; offset < sample_bag.length(); ) {
        const auto len
          = *reinterpret_cast<const uint32_t*>( sample_bag.data() + offset );
        offset += 4;

        samples.emplace_back();
        samples.back().Deserialize( sample_bag.data() + offset, len );
        offset += len;
      }

      pbrt::graphics::AccumulateImage( scene_.camera, samples );
    } while ( bags_queue.try_dequeue( sample_bag ) );
  }
}

void Accumulator::run()
{
  while ( loop_.wait_next_event( -1 ) != EventLoop::Result::Exit ) {
    continue;
  }
}

int main( int argc, char* argv[] )
{
  try {
    if ( argc <= 0 ) {
      abort();
    }

    if ( argc != 2 ) {
      usage( argv[0] );
      return EXIT_FAILURE;
    }

    FLAGS_logtostderr = false;

    const uint16_t port = static_cast<uint16_t>( stoi( argv[1] ) );
    Accumulator accumulator { port };
    accumulator.run();
  } catch ( const exception& e ) {
    print_exception( argv[0], e );
    return EXIT_FAILURE;
  }

  return 0;
}
