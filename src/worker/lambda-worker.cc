#include "lambda-worker.hh"

#include <filesystem>
#include <getopt.h>
#include <signal.h>

#include "messages/utils.hh"
#include "net/transfer_mcd.hh"
#include "net/transfer_s3.hh"

using namespace std;
using namespace chrono;
using namespace r2t2;
using namespace pbrt;
using namespace meow;

using OpCode = Message::OpCode;

LambdaWorker::LambdaWorker( const string& coordinator_ip,
                            const uint16_t coordinator_port,
                            const string& storage_uri,
                            const WorkerConfiguration& config )
  : config( config )
  , coordinator_addr( coordinator_ip, coordinator_port )
  , master_connection( [this] {
    TCPSocket socket;
    socket.set_blocking( false );
    socket.connect( this->coordinator_addr );
    return socket;
  }() )
  , working_directory( "/tmp/r2t2-worker" )
  , storage_backend( StorageBackend::create_backend( storage_uri ) )
  , worker_rule_categories( { loop.add_category( "Socket" ),
                              loop.add_category( "Message read" ),
                              loop.add_category( "Message write" ),
                              loop.add_category( "Process message" ) } )
{
  // let the program handle SIGPIPE
  signal( SIGPIPE, SIG_IGN );

  if ( !config.memcached_servers.empty() ) {
    transfer_agent
      = make_unique<memcached::TransferAgent>( config.memcached_servers );
  } else {
    transfer_agent = make_unique<S3TransferAgent>( storage_backend );
  }

  samples_transfer_agent
    = make_unique<S3TransferAgent>( storage_backend, 2, true );

  cerr << "* starting worker in " << working_directory.name() << endl;
  filesystem::current_path( working_directory.name() );

  FLAGS_log_dir = ".";
  FLAGS_log_prefix = false;
  google::InitGoogleLogging( log_base.c_str() );

  if ( track_rays ) {
    TLOG( RAY ) << "timestamp,pathId,hop,shadowRay,remainingBounces,workerId,"
                   "treeletId,action,bag";
  }

  if ( track_bags ) {
    TLOG( BAG ) << "timestamp,bagTreeletId,bagWorkerId,bagId,thisWorkerId,"
                   "count,size,action";
  }

  pbrt::PbrtOptions.nThreads = 1;

  scene.samples_per_pixel = config.samples_per_pixel;
  scene.max_depth = config.max_path_depth;

  /* trace rays */
  loop.add_rule( "Trace queue",
                 bind( &LambdaWorker::handle_trace_queue, this ),
                 [this] { return !trace_queue.empty(); } );

  /* create ray packets */
  loop.add_rule( "Out queue",
                 bind( &LambdaWorker::handle_out_queue, this ),
                 [this] { return out_queue_size > 0; } );

  loop.add_rule( "Samples",
                 bind( &LambdaWorker::handle_samples, this ),
                 [this] { return !samples.empty(); } );

  loop.add_rule( "Open bags",
                 Direction::In,
                 seal_bags_timer,
                 bind( &LambdaWorker::handle_open_bags, this ),
                 [this] { return !open_bags.empty(); } );

  loop.add_rule( "Sealed bags",
                 bind( &LambdaWorker::handle_sealed_bags, this ),
                 [this] { return !sealed_bags.empty(); } );

  loop.add_rule( "Sample bags",
                 Direction::In,
                 sample_bags_timer,
                 bind( &LambdaWorker::handle_sample_bags, this ),
                 [this] { return !sample_bags.empty(); } );

  loop.add_rule( "Receive queue",
                 bind( &LambdaWorker::handle_receive_queue, this ),
                 [this] { return !receive_queue.empty(); } );

  loop.add_rule( "Transfer agent",
                 Direction::In,
                 transfer_agent->eventfd(),
                 bind( &LambdaWorker::handle_transfer_results, this, false ),
                 [this] { return !pending_ray_bags.empty(); } );

  loop.add_rule( "Samples agent",
                 Direction::In,
                 samples_transfer_agent->eventfd(),
                 bind( &LambdaWorker::handle_transfer_results, this, true ),
                 [this] { return !pending_ray_bags.empty(); } );

  loop.add_rule( "Worker stats",
                 Direction::In,
                 worker_stats_timer,
                 bind( &LambdaWorker::handle_worker_stats, this ),
                 [this] { return true; } );

  master_connection.install_rules(
    loop,
    worker_rule_categories,
    [this]( meow::Message&& msg ) { this->process_message( msg ); },
    [this] { this->terminate(); } );
}

void LambdaWorker::get_objects( const protobuf::GetObjects& objects )
{
  vector<storage::GetRequest> requests;

  for ( const protobuf::ObjectKey& object_key : objects.object_ids() ) {
    const ObjectKey id = from_protobuf( object_key );
    if ( id.type == ObjectType::TriangleMesh ) {
      /* triangle meshes are packed into treelets, so ignore */
      continue;
    }

    if ( id.type == ObjectType::Treelet ) {
      treelets.emplace( id.id, make_unique<CloudBVH>( id.id ) );
    }

    const string file_path = scene::GetObjectName( id.type, id.id );
    requests.emplace_back( file_path, file_path );
  }

  storage_backend->get( requests );
}

void LambdaWorker::run()
{
  while ( !terminated
          && loop.wait_next_event( -1 ) != EventLoop::Result::Exit ) {
    continue;
  }
}

void usage( const char* argv0, int exitCode )
{
  cerr << "Usage: " << argv0 << " [OPTIONS]" << endl
       << endl
       << "Options:" << endl
       << "  -i --ip IPSTRING           ip of coordinator" << endl
       << "  -p --port PORT             port of coordinator" << endl
       << "  -s --storage-backend NAME  storage backend URI" << endl
       << "  -S --samples N             number of samples per pixel" << endl
       << "  -M --max-depth N           maximum path depth" << endl
       << "  -L --log-rays RATE         log ray actions" << endl
       << "  -B --log-bags RATE         log bag actions" << endl
       << "  -d --memcached-server      address for memcached" << endl
       << "  -h --help                  show help information" << endl;

  exit( exitCode );
}

int main( int argc, char* argv[] )
{
  int exit_status = EXIT_SUCCESS;

  uint16_t listen_port = 50000;
  string public_ip;
  string storage_uri;

  int samples_per_pixel = 0;
  int max_path_depth = 0;
  float ray_log_rate = 0.0;
  float bag_log_rate = 0.0;

  vector<Address> memcached_servers;

  struct option long_options[] = {
    { "port", required_argument, nullptr, 'p' },
    { "ip", required_argument, nullptr, 'i' },
    { "storage-backend", required_argument, nullptr, 's' },
    { "samples", required_argument, nullptr, 'S' },
    { "max-depth", required_argument, nullptr, 'M' },
    { "log-rays", required_argument, nullptr, 'L' },
    { "log-bags", required_argument, nullptr, 'B' },
    { "directional", no_argument, nullptr, 'I' },
    { "memcached-server", required_argument, nullptr, 'd' },
    { "help", no_argument, nullptr, 'h' },
    { nullptr, 0, nullptr, 0 },
  };

  while ( true ) {
    const int opt
      = getopt_long( argc, argv, "p:i:s:S:M:L:B:d:hI", long_options, nullptr );

    if ( opt == -1 )
      break;

    // clang-format off
    switch (opt) {
    case 'p': listen_port = stoi(optarg); break;
    case 'i': public_ip = optarg; break;
    case 's': storage_uri = optarg; break;
    case 'S': samples_per_pixel = stoi(optarg); break;
    case 'M': max_path_depth = stoi(optarg); break;
    case 'L': ray_log_rate = stof(optarg); break;
    case 'B': bag_log_rate = stof(optarg); break;
    case 'I': PbrtOptions.directionalTreelets = true; break;
    case 'h': usage(argv[0], EXIT_SUCCESS); break;

    case 'd': {
        string host;
        uint16_t port = 11211;
        tie(host, port) = Address::decompose(optarg);
        memcached_servers.emplace_back(host, port);
        break;
    }

    default: usage(argv[0], EXIT_FAILURE);
    }
    // clang-format on
  }

  if ( listen_port == 0 || samples_per_pixel < 0 || max_path_depth < 0
       || ray_log_rate < 0 || ray_log_rate > 1.0 || bag_log_rate < 0
       || bag_log_rate > 1.0 || public_ip.empty() || storage_uri.empty() ) {
    usage( argv[0], EXIT_FAILURE );
  }

  unique_ptr<LambdaWorker> worker;
  WorkerConfiguration config { samples_per_pixel,
                               max_path_depth,
                               ray_log_rate,
                               bag_log_rate,
                               move( memcached_servers ) };

  try {
    worker = make_unique<LambdaWorker>(
      public_ip, listen_port, storage_uri, config );
    worker->run();
  } catch ( const exception& e ) {
    cerr << argv[0] << ": " << e.what() << endl;
    exit_status = EXIT_FAILURE;
  }

  if ( worker ) {
    worker->upload_logs();
  }

  return exit_status;
}
