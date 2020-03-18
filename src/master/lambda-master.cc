#include "lambda-master.hh"

#include <getopt.h>
#include <glog/logging.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <ctime>
#include <deque>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <numeric>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "execution/meow/message.hh"
#include "messages/utils.hh"
#include "net/lambda.hh"
#include "net/requests.hh"
#include "net/socket.hh"
#include "net/transfer_mcd.hh"
#include "net/util.hh"
#include "schedulers/dynamic.hh"
#include "schedulers/null.hh"
#include "schedulers/rootonly.hh"
#include "schedulers/static.hh"
#include "schedulers/uniform.hh"
#include "util/exception.hh"
#include "util/path.hh"
#include "util/random.hh"
#include "util/status_bar.hh"
#include "util/temp_file.hh"
#include "util/tokenize.hh"
#include "util/uri.hh"
#include "util/util.hh"

using namespace std;
using namespace chrono;
using namespace meow;
using namespace r2t2;
using namespace pbrt;

using OpCode = Message::OpCode;
using PollerResult = Poller::Result::Type;

map<LambdaMaster::Worker::Role, size_t> LambdaMaster::Worker::active_count = {};
WorkerId LambdaMaster::Worker::next_id = 0;

LambdaMaster::~LambdaMaster()
{
  try {
    roost::empty_directory( sceneDir.name() );
  } catch ( exception& ex ) {
  }
}

LambdaMaster::LambdaMaster( const uint16_t listen_port,
                            const uint16_t client_port,
                            const uint32_t max_workers,
                            const uint32_t ray_generators,
                            const string& public_address,
                            const string& storage_backend_uri,
                            const string& aws_region,
                            unique_ptr<Scheduler>&& scheduler,
                            const MasterConfiguration& config )
  : config( config )
  , job_id( [] {
    ostringstream oss;
    oss << hex << setfill( '0' ) << setw( 8 ) << time( nullptr );
    return oss.str();
  }() )
  , max_workers( max_workers )
  , ray_generators( ray_generators )
  , scheduler( move( scheduler ) )
  , public_address( public_address )
  , storage_backend_uri( storage_backend_uri )
  , storage_backend( StorageBackend::create_backend( storage_backend_uri ) )
  , aws_region( aws_region )
  , aws_address( LambdaInvocationRequest::endpoint( aws_region ), "https" )
  , worker_stats_write_timer( seconds { config.worker_stats_write_interval },
                              milliseconds { 1 } )
{
  signals.set_as_mask();

  const string scene_path = scene_dir.name();
  roost::create_directories( scene_path );

  protobuf::InvocationPayload invocation_proto;
  invocation_proto.set_storage_backend( storage_backend_uri );
  invocation_proto.set_coordinator( public_address );
  invocation_proto.set_samples_per_pixel( config.samples_per_pixel );
  invocation_proto.set_max_path_depth( config.max_path_depth );
  invocation_proto.set_ray_log_rate( config.ray_log_rate );
  invocation_proto.set_bag_log_rate( config.bag_log_rate );
  invocation_proto.set_directional_treelets( PbrtOptions.directionalTreelets );

  for ( const auto& server : config.memcached_servers ) {
    *invocation_proto.add_memcached_servers() = server;
  }

  invocation_payload = protoutil::to_json( invocation_proto );

  /* download required scene objects from the bucket */
  auto get_scene_object_request = [&scene_path]( const ObjectType type ) {
    return storage::GetRequest { scene::GetObjectName( type, 0 ),
                                 roost::path( scene_path )
                                   / scene::GetObjectName( type, 0 ) };
  };

  vector<storage::GetRequest> scene_obj_reqs {
    get_scene_object_request( ObjectType::Manifest ),
    get_scene_object_request( ObjectType::Camera ),
    get_scene_object_request( ObjectType::Sampler ),
    get_scene_object_request( ObjectType::Lights ),
    get_scene_object_request( ObjectType::Scene ),
  };

  cerr << "Downloading scene data... ";
  storage_backend->get( scene_obj_reqs );
  cerr << "done." << endl;

  /* now we can initialize the scene */
  scene = { scene_path, config.samples_per_pixel, config.crop_window };

  /* initializing the treelets array */
  const size_t treelet_count = scene.base.GetTreeletCount();
  treelets.reserve( treelet_count );
  treelet_stats.reserve( treelet_count );

  for ( size_t i = 0; i < treelet_count; i++ ) {
    treelets.emplace_back( i );
    treelet_stats.emplace_back();
    unassigned_treelets.insert( i );
  }

  queued_ray_bags.resize( treelet_count );
  pending_ray_bags.resize( treelet_count );

  tiles = Tiles { config.tile_size,
                  scene.sampleBounds,
                  scene.base.samplesPerPixel,
                  ray_generators ? ray_generators : max_workers };

  /* are we logging anything? */
  if ( config.collect_debug_logs || config.worker_stats_write_interval > 0
       || config.ray_log_rate > 0 || config.bag_log_rate > 0 ) {
    roost::create_directories( config.logs_directory );
  }

  if ( config.worker_stats_write_interval > 0 ) {
    ws_stream.open( config.logs_directory + "/" + "workers.csv", ios::trunc );
    tl_stream.open( config.logs_directory + "/" + "treelets.csv", ios::trunc );

    ws_stream << "timestamp,workerId,pathsFinished,"
                 "raysEnqueued,raysAssigned,raysDequeued,"
                 "bytesEnqueued,bytesAssigned,bytesDequeued,"
                 "bagsEnqueued,bagsAssigned,bagsDequeued,"
                 "numSamples,bytesSamples,bagsSamples,cpuUsage\n";

    tl_stream << "timestamp,treeletId,raysEnqueued,raysDequeued,"
                 "bytesEnqueued,bytesDequeued,bagsEnqueued,bagsDequeued\n";
  }

  auto print_info = []( char const* key, auto value ) {
    cerr << "  " << key << "    \e[1m" << value << "\e[0m" << endl;
  };

  cerr << endl << "Job info:" << endl;
  print_info( "Job ID           ", job_id );
  print_info( "Working directory", scene_path );
  print_info( "Public address   ", public_address );
  print_info( "Maxium workers   ", max_workers );
  print_info( "Ray generators   ", ray_generators );
  print_info( "Treelet count    ", treelet_count );
  print_info( "Tile size        ",
              to_string( tiles.tile_size ) + "\u00d7"
                + to_string( tiles.tile_size ) );
  print_info( "Output dimensions",
              to_string( scene.sampleExtent.x ) + "\u00d7"
                + to_string( scene.sampleExtent.y ) );
  print_info( "Samples per pixel", scene.samplesPerPixel );
  print_info( "Total paths      ",
              scene.sampleExtent.x * scene.sampleExtent.y
                * scene.samplesPerPixel );
  cerr << endl;

  loop.add_rule(
    "Reschedule",
    reschedule_timer,
    Direction::In,
    bind( &LambdaMaster::handle_reschedule, this ),
    [this] { return finished_ray_generators == this->ray_generators; } );

  loop.add_rule(
    job_exit_timer,
    Direction::In,
    [&] { terminated = true; },
    [] { return true; }, );

  loop.add_rule( "Messages",
                 bind( &LambdaMaster::handle_messages, this ),
                 [this] { return !incoming_messages.empty(); } );

  loop.add_rule(
    "Queued ray bags" bind( &LambdaMaster::handle_queued_ray_bags, this ),
    [this] {
      return initialized_workers >= this->max_workers && !free_workers.empty()
             && ( tiles.cameraRaysRemaining() || queued_ray_bags_count > 0 );
    } );

  if ( config.worker_stats_write_interval > 0 ) {
    loop.add_rule( "Worker stats",
                   worker_stats_write_timer,
                   Direction::In,
                   bind( &LambdaMaster::handle_worker_stats, this ),
                   [this] { return true; } );
  }

  loop.add_rule( "Worker invocation",
                 worker_invocation_timer,
                 Direction::In,
                 bind( &LambdaMaster::handle_worker_invocation, this ),
                 [this] {
                   return !treelets_to_spawn.empty()
                          && ( Worker::active_count[Worker::Role::Tracer]
                               < this->max_workers );
                 } );

  loop.add_rule( "Status",
                 status_print_timer,
                 Direction::In,
                 bind( &LambdaMaster::handle_status_message, this ),
                 [this]() { return true; } );

  listener_socket.set_blocking( false );
  listener_socket.set_reuseaddr();
  listener_socket.bind( { "0.0.0.0", listen_port } );
  listener_socket.listen();

  loop.add_rule(
    "Listener",
    [this, max_workers] {
      TCPSocket socket = listener_socket.accept();

      /* do we want this worker? */
      if ( Worker::next_id >= this->ray_generators
           && treelets_to_spawn.empty() ) {
        socket.close();
        return true;
      }

      const WorkerId worker_id = Worker::next_id++;

      auto connection_close_handler = [this, worker_id] {
        auto& worker = workers[worker_id];

        if ( worker.state == Worker::State::Terminating ) {
          if ( worker.role == Worker::Role::Generator ) {
            last_generator_done = steady_clock::now();
            finished_ray_generators++;
          }

          /* it's okay for this worker to go away,
             let's not panic! */
          worker.state = Worker::State::Terminated;
          Worker::active_count[worker.role]--;

          if ( !worker.outstanding_ray_bags.empty() ) {
            throw runtime_error( "worker died without finishing its work: "
                                 + to_string( workerId ) );
          }

          return;
        }

        cerr << "Worker info: " << worker.to_string() << endl;

        throw runtime_error(
          "worker died unexpectedly: " + to_string( workerId )
          + ( worker.aws_log_stream.empty()
                ? ""s
                : ( " ("s + worker.aws_log_stream + ")"s ) ) );
      };

      if ( worker_id < this->ray_generators ) {
        /* This worker is a ray generator
           Let's (1) say hi, (2) tell the worker to fetch the scene,
           (3) generate rays for its tile */

        /* (0) create the entry for the worker */
        auto& worker = workers.emplace_back(
          worker_id, Worker::Role::Generator, move( socket ) );

        assign_base_objects( worker );

        /* (1) saying hi, assigning id to the worker */
        protobuf::Hey hey_proto;
        hey_proto.set_worker_id( worker_id );
        hey_proto.set_job_id( job_id );
        worker.connection->enqueue_write(
          Message::str( 0, OpCode::Hey, protoutil::to_string( hey_proto ) ) );

        /* (2) tell the worker to get the necessary scene objects */
        protobuf::GetObjects objs_proto;
        for ( const ObjectKey& id : worker.objects ) {
          *objs_proto.add_object_ids() = to_protobuf( id );
        }

        worker.connection->enqueue_write( Message::str(
          0, OpCode::GetObjects, protoutil::to_string( objs_proto ) ) );

        /* (3) Tell the worker to generate rays */
        if ( tiles.camera_rays_remaining() ) {
          tiles.send_worker_tile( worker );
        } else {
          /* Too many ray launchers for tile size,
           * so just finish immediately */
          worker.connection->enqueue_write(
            Message::str( 0, OpCode::FinishUp, "" ) );
          worker.state = Worker::State::FinishingUp;
        }
      } else {
        /* this is a normal worker */
        if ( !treelets_to_spawn.empty() ) {
          const TreeletId treelet_id = treelets_to_spawn.front();
          treelets_to_spawn.pop_front();

          auto& treelet = treelets[treelet_id];
          treelet.pending_workers--;

          /* (0) create the entry for the worker */
          auto& workers.emplace_back(
            workerId, Worker::Role::Tracer, move( socket ) );

          assign_base_objects( worker );
          assign_treelet( worker, treelet );

          /* (1) saying hi, assigning id to the worker */
          protobuf::Hey hey_proto;
          hey_proto.set_worker_id( worker_id );
          hey_proto.set_job_id( job_id );
          worker.connection->enqueue_write(
            Message::str( 0, OpCode::Hey, protoutil::to_string( hey_proto ) ) );

          /* (2) tell the worker to get the scene objects necessary */
          protobuf::GetObjects objs_proto;
          for ( const ObjectKey& id : worker.objects ) {
            *objs_proto.add_object_ids() = to_protobuf( id );
          }

          worker.connection->enqueue_write( Message::str(
            0, OpCode::GetObjects, protoutil::to_string( objs_proto ) ) );

          free_workers.push_back( worker.id );
        } else {
          throw runtime_error( "we accepted a useless worker" );
        }
      }

      workers.back().session.install_rules( loop, connection_close_handler );
    },

    [] { return true; },
    [] { throw runtime_error( "listener socket closed" ); } );

  if ( client_port != 0 ) {
    throw runtime_error( "client support is disabled" );
  }
}

string LambdaMaster::Worker::to_string() const
{
  ostringstream oss;

  size_t oustanding_size = 0;
  for ( const auto& bag : outstanding_ray_bags )
    oustanding_size += bag.bag_size;

  oss << "id=" << id << ",state=" << static_cast<int>( state )
      << ",role=" << static_cast<int>( role ) << ",awslog=" << aws_log_stream
      << ",treelets=";

  for ( auto it = treelets.begin(); it != treelets.end(); it++ ) {
    if ( it != treelets.begin() )
      oss << ",";
    oss << *it;
  }

  oss << ",outstanding=" << outstanding_ray_bags.size()
      << ",outstanding-bytes=" << oustanding_size
      << ",enqueued=" << stats.enqueued.bytes
      << ",assigned=" << stats.assigned.bytes
      << ",dequeued=" << stats.dequeued.bytes
      << ",samples=" << stats.samples.bytes;

  return oss.str();
}

void LambdaMaster::run()
{
  StatusBar::get();

  if ( !config.memcached_servers.empty() ) {
    cerr << "Flushing " << config.memcached_servers.size() << " memcached "
         << pluralize( "server", config.memcached_servers.size() ) << "... ";

    vector<Address> servers;

    for ( auto& server : config.memcached_servers ) {
      string host;
      uint16_t port = 11211;
      tie( host, port ) = Address::decompose( server );
      servers.emplace_back( host, port );
    }

    EventLoop memcached_loop;
    unique_ptr<memcached::TransferAgent> agent
      = make_unique<memcached::TransferAgent>( servers, servers.size() );

    size_t flushed_count = 0;

    memcached_loop.add_rule(
      "eventfd",
      agent->eventfd(),
      Direction::In,
      [&] {
        if ( !agent->eventfd().read_event() )
          return ResultType::Continue;

        vector<pair<uint64_t, string>> actions;
        agent->tryPopBulk( back_inserter( actions ) );

        flushed_count += actions.size();
      },
      [&] { return flushed_count < config.memcached_servers.size(); } );

    agent->flushAll();

    while ( memcached_loop.wait_next_event( -1 ) != EventLoop::Result::Exit ) {
      continue;
    }

    cerr << "done." << endl;
  }

  if ( ray_generators > 0 ) {
    /* let's invoke the ray generators */
    cerr << "Launching " << ray_generators << " ray "
         << pluralize( "generator", ray_generators ) << "... ";

    invokeWorkers( ray_generators
                   + static_cast<size_t>( 0.1 * ray_generators ) );
    cerr << "done." << endl;
  }

  while ( !terminated
          && loop.wait_next_event( -1 ) != EventLoop::Result::Exit ) {
    continue;
  }

  vector<storage::GetRequest> get_requests;
  const string log_prefix = "logs/" + jobId + "/";

  ws_stream.close();
  tl_stream.close();

  for ( auto& worker : workers ) {
    if ( worker.state != Worker::State::Terminated ) {
      worker.connection->socket().close();
    }

    if ( config.collect_debug_logs || config.ray_log_rate
         || config.bag_log_rate ) {
      get_requests.emplace_back( logPrefix + to_string( worker.id ) + ".INFO",
                                 config.logs_directory + "/"
                                   + to_string( worker.id ) + ".INFO" );
    }
  }

  cerr << endl;

  print_job_summary();

  if ( !config.job_summary_path.empty() ) {
    dump_job_summary();
  }

  if ( !get_requests.empty() ) {
    cerr << "\nDownloading " << get_requests.size() << " log file(s)... ";
    this_thread::sleep_for( 10s );
    storage_backend->get( get_requests );
    cerr << "done." << endl;
  }
}

void usage( const char* argv0, int exit_code )
{
  cerr << "Usage: " << argv0 << " [OPTION]..." << endl
       << endl
       << "Options:" << endl
       << "  -p --port PORT             port to use" << endl
       << "  -P --client-port PORT      port for clients to connect" << endl
       << "  -i --ip IPSTRING           public ip of this machine" << endl
       << "  -r --aws-region REGION     region to run lambdas in" << endl
       << "  -b --storage-backend NAME  storage backend URI" << endl
       << "  -m --max-workers N         maximum number of workers" << endl
       << "  -G --ray-generators N      number of ray generators" << endl
       << "  -g --debug-logs            collect worker debug logs" << endl
       << "  -w --worker-stats N        log worker stats every N seconds"
       << endl
       << "  -L --log-rays RATE         log ray actions" << endl
       << "  -B --log-bags RATE         log bag actions" << endl
       << "  -D --logs-dir DIR          set logs directory (default: logs/)"
       << endl
       << "  -S --samples N             number of samples per pixel" << endl
       << "  -M --max-depth N           maximum path depth" << endl
       << "  -a --scheduler TYPE        indicate scheduler type:" << endl
       << "                               - uniform (default)" << endl
       << "                               - static" << endl
       << "                               - all" << endl
       << "                               - none" << endl
       << "  -c --crop-window X,Y,Z,T   set render bounds to [(X,Y), (Z,T))"
       << endl
       << "  -T --pix-per-tile N        pixels per tile (default=44)" << endl
       << "  -n --new-tile-send N       threshold for sending new tiles" << endl
       << "  -t --timeout T             exit after T seconds of inactivity"
       << endl
       << "  -j --job-summary FILE      output the job summary in JSON format"
       << endl
       << "  -d --memcached-server      address for memcached" << endl
       << "                             (can be repeated)" << endl
       << "  -h --help                  show help information" << endl;

  exit( exit_code );
}

Optional<Bounds2i> parse_crop_window_optarg( const string& optarg )
{
  vector<string> args = split( optarg, "," );
  if ( args.size() != 4 )
    return {};

  Point2i p_min, p_max;
  p_min.x = stoi( args[0] );
  p_min.y = stoi( args[1] );
  p_max.x = stoi( args[2] );
  p_max.y = stoi( args[3] );

  return { true, Bounds2i { p_min, p_max } };
}

int main( int argc, char* argv[] )
{
  if ( argc <= 0 ) {
    abort();
  }

  timer();

  google::InitGoogleLogging( argv[0] );

  uint16_t listen_port = 50000;
  uint16_t client_port = 0;
  int32_t max_workers = -1;
  int32_t ray_generators = 0;
  string public_ip;
  string storage_backend_uri;
  string region { "us-west-2" };
  uint64_t worker_stats_write_interval = 0;
  bool collect_debug_logs = false;
  float ray_log_rate = 0.0;
  float bag_log_rate = 0.0;
  string logs_directory = "logs/";
  Optional<Bounds2i> crop_window;
  uint32_t timeout = 0;
  uint32_t pixels_per_tile = 0;
  uint64_t new_tile_threshold = 10000;
  string job_summary_path;

  unique_ptr<Scheduler> scheduler = nullptr;
  string scheduler_name;

  int samples_per_pixel = 0;
  int max_path_depth = 5;
  int tile_size = 0;

  uint32_t max_jobs_on_engine = 1;
  vector<string> memcached_servers;
  vector<pair<string, uint32_t>> engines;

  struct option long_options[] = {
    { "port", required_argument, nullptr, 'p' },
    { "client-port", required_argument, nullptr, 'P' },
    { "ip", required_argument, nullptr, 'i' },
    { "aws-region", required_argument, nullptr, 'r' },
    { "storage-backend", required_argument, nullptr, 'b' },
    { "max-workers", required_argument, nullptr, 'm' },
    { "ray-generators", required_argument, nullptr, 'G' },
    { "scheduler", required_argument, nullptr, 'a' },
    { "debug-logs", no_argument, nullptr, 'g' },
    { "worker-stats", required_argument, nullptr, 'w' },
    { "log-rays", required_argument, nullptr, 'L' },
    { "log-bags", required_argument, nullptr, 'B' },
    { "logs-dir", required_argument, nullptr, 'D' },
    { "samples", required_argument, nullptr, 'S' },
    { "max-depth", required_argument, nullptr, 'M' },
    { "crop-window", required_argument, nullptr, 'c' },
    { "timeout", required_argument, nullptr, 't' },
    { "job-summary", required_argument, nullptr, 'j' },
    { "pix-per-tile", required_argument, nullptr, 'T' },
    { "new-tile-send", required_argument, nullptr, 'n' },
    { "directional", no_argument, nullptr, 'I' },
    { "jobs", required_argument, nullptr, 'J' },
    { "memcached-server", required_argument, nullptr, 'd' },
    { "engine", required_argument, nullptr, 'E' },
    { "help", no_argument, nullptr, 'h' },
    { nullptr, 0, nullptr, 0 },
  };

  while ( true ) {
    const int opt
      = getopt_long( argc,
                     argv,
                     "p:P:i:r:b:m:G:w:D:a:S:M:L:c:t:j:T:n:J:d:E:B:gh",
                     long_options,
                     nullptr );

    if ( opt == -1 ) {
      break;
    }

    switch ( opt ) {
        // clang-format off
        case 'p': listen_port = stoi(optarg); break;
        case 'P': client_port = stoi(optarg); break;
        case 'i': public_ip = optarg; break;
        case 'r': region = optarg; break;
        case 'b': storage_backend_uri = optarg; break;
        case 'm': max_workers = stoi(optarg); break;
        case 'G': ray_generators = stoi(optarg); break;
        case 'a': scheduler_name = optarg; break;
        case 'g': collect_debug_logs = true; break;
        case 'w': worker_stats_write_interval = stoul(optarg); break;
        case 'D': logs_directory = optarg; break;
        case 'S': samples_per_pixel = stoi(optarg); break;
        case 'M': max_path_depth = stoi(optarg); break;
        case 'L': ray_log_rate = stof(optarg); break;
        case 'B': bag_log_rate = stof(optarg); break;
        case 't': timeout = stoul(optarg); break;
        case 'j': job_summary_path = optarg; break;
        case 'n': new_tile_threshold = stoull(optarg); break;
        case 'I': PbrtOptions.directionalTreelets = true; break;
        case 'J': max_jobs_on_engine = stoul(optarg); break;
        case 'd': memcached_servers.emplace_back(optarg); break;
        case 'E': engines.emplace_back(optarg, max_jobs_on_engine); break;
        case 'h': usage(argv[0], EXIT_SUCCESS); break;

        // clang-format on

      case 'T': {
        if ( strcmp( optarg, "auto" ) == 0 ) {
          tile_size = numeric_limits<typeof( tile_size )>::max();
          pixels_per_tile = numeric_limits<typeof( pixels_per_tile )>::max();
        } else {
          pixels_per_tile = stoul( optarg );
          tile_size = ceil( sqrt( pixels_per_tile ) );
        }
        break;
      }

      case 'c':
        crop_window = parse_crop_window_optarg( optarg );

        if ( !crop_window.initialized() ) {
          cerr << "Error: bad crop window (" << optarg << ")." << endl;
          usage( argv[0], EXIT_FAILURE );
        }

        break;

      default:
        usage( argv[0], EXIT_FAILURE );
        break;
    }
  }

  if ( scheduler_name == "uniform" ) {
    scheduler = make_unique<UniformScheduler>();
  } else if ( scheduler_name == "static" ) {
    auto storage = StorageBackend::create_backend( storage_backend_uri );
    TempFile static_file { "/tmp/r2t2-lambda-master.STATIC0" };

    cerr << "Downloading static assignment file... ";
    storage->get( { { scene::GetObjectName( ObjectType::StaticAssignment, 0 ),
                      static_file.name() } } );
    cerr << "done." << endl;

    scheduler = make_unique<StaticScheduler>( static_file.name() );
  } else if ( scheduler_name == "dynamic" ) {
    scheduler = make_unique<DynamicScheduler>();
  } else if ( scheduler_name == "rootonly" ) {
    scheduler = make_unique<RootOnlyScheduler>();
  } else if ( scheduler_name == "null" ) {
    scheduler = make_unique<NullScheduler>();
  } else {
    usage( argv[0], EXIT_FAILURE );
  }

  if ( scheduler == nullptr || listen_port == 0 || max_workers <= 0
       || ray_generators < 0 || samples_per_pixel < 0 || max_path_depth < 0
       || ray_log_rate < 0 || ray_log_rate > 1.0 || bag_log_rate < 0
       || bag_log_rate > 1.0 || public_ip.empty() || storage_backend_uri.empty()
       || region.empty() || new_tile_threshold == 0
       || ( crop_window.initialized() && pixels_per_tile != 0
            && pixels_per_tile
                 != numeric_limits<typeof( pixels_per_tile )>::max()
            && pixels_per_tile > crop_window->Area() ) ) {
    usage( argv[0], 2 );
  }

  ostringstream public_address;
  public_address << public_ip << ":" << listen_port;

  unique_ptr<LambdaMaster> master;

  // TODO clean this up
  MasterConfiguration config = { samples_per_pixel,
                                 max_path_depth,
                                 collect_debug_logs,
                                 worker_stats_write_interval,
                                 ray_log_rate,
                                 bag_log_rate,
                                 logs_directory,
                                 crop_window,
                                 tile_size,
                                 seconds { timeout },
                                 job_summary_path,
                                 new_tile_threshold,
                                 move( memcached_servers ),
                                 move( engines ) };

  try {
    master = make_unique<LambdaMaster>( listen_port,
                                        client_port,
                                        max_workers,
                                        ray_generators,
                                        public_address.str(),
                                        storage_backend_uri,
                                        region,
                                        move( scheduler ),
                                        config );

    master->run();

    cerr << endl << timer().summary() << endl;
  } catch ( const exception& e ) {
    print_exception( argv[0], e );
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}
