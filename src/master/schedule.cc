#include <algorithm>
#include <chrono>
#include <iomanip>
#include <iterator>
#include <numeric>
#include <random>

#include "lambda-master.hh"
#include "messages/message.hh"
#include "messages/utils.hh"
#include "net/http_client.hh"
#include "net/lambda.hh"
#include "net/session.hh"
#include "schedulers/scheduler.hh"
#include "util/exception.hh"
#include "util/random.hh"

using namespace std;
using namespace chrono;
using namespace r2e2;
using namespace meow;

using OpCode = Message::OpCode;

void LambdaMaster::invoke_workers( const size_t n_workers )
{
  if ( n_workers == 0 )
    return;

  uniform_int_distribution<size_t> region_selector( 0, aws_regions.size() - 1 );

  if ( config.engines.empty() ) {
    for ( size_t i = 0; i < n_workers; i++ ) {
      // launch all ray generators & accumulators in the first region
      const auto region_idx = ( finished_ray_generators == this->ray_generators
                                and started_accumulators == this->accumulators )
                                ? region_selector( rand_engine )
                                : 0;

      HTTPRequest invocation_request
        = LambdaInvocationRequest(
            aws_credentials,
            aws_regions.at( region_idx ).name,
            lambda_function_name,
            invocation_payload,
            LambdaInvocationRequest::InvocationType::EVENT,
            LambdaInvocationRequest::LogType::NONE )
            .to_http_request();

      TCPSocket socket;
      socket.set_blocking( false );
      socket.connect( aws_regions.at( region_idx ).address );
      https_clients.emplace_back(
        SSLSession { ssl_context.make_SSL_handle(), move( socket ) } );

      auto client_it = prev( https_clients.end() );

      client_it->install_rules(
        loop,
        https_rule_categories,
        [client_it, this]( HTTPResponse&& ) {
          finished_https_clients.push_back( client_it );
          return true;
        },
        [] {},
        [] {} );

      client_it->push_request( move( invocation_request ) );
    }
  } else {
    throw runtime_error( "external engines not implemented" );
  }
}

void LambdaMaster::handle_reschedule()
{
  reschedule_timer.read_event();

  /* (1) call the schedule function */

  auto start = steady_clock::now();
  auto schedule = scheduler->schedule(
    max_workers, treelet_stats, aggregated_stats, scene.total_paths );

  if ( schedule ) {
    cerr << "\u2192 Rescheduling... ";

    execute_schedule( *schedule );
    auto end = steady_clock::now();

    cerr << "done (" << fixed << setprecision( 2 )
         << duration_cast<milliseconds>( end - start ).count() << " ms)."
         << endl;
  }
}

void LambdaMaster::handle_worker_invocation()
{
  worker_invocation_timer.read_event();

  /* let's start as many workers as we can right now */
  const auto running_count = Worker::active_count[Worker::Role::Tracer];
  const size_t available_capacity
    = ( this->max_workers > running_count )
        ? static_cast<size_t>( this->max_workers - running_count )
        : 0ul;

  invoke_workers( min( available_capacity, treelets_to_spawn.size() ) );
}

void LambdaMaster::execute_schedule( const Schedule& schedule )
{
  /* is the schedule viable? */
  if ( schedule.size() != treelets.size() ) {
    throw runtime_error( "invalid schedule" );
  }

  const auto total_requested_workers
    = accumulate( schedule.begin(), schedule.end(), 0ull );

  if ( total_requested_workers > max_workers ) {
    throw runtime_error( "not enough workers available for the schedule" );
  }

  /* let's plan */
  vector<WorkerId> workers_to_take_down;
  treelets_to_spawn.clear();

  for ( TreeletId tid = 0; tid < treelets.size(); tid++ ) {
    const size_t requested = schedule[tid];
    const size_t current = treelets[tid].workers.size();

    if ( requested == current ) {
      continue;
    } else if ( requested > current ) {
      /* we need to start new workers */
      treelets[tid].pending_workers = requested - current;
      treelets_to_spawn.insert(
        treelets_to_spawn.end(), requested - current, tid );
    } else /* (requested < current) */ {
      auto& treelet_workers = treelets[tid].workers;
      for ( size_t i = 0; i < current - requested; i++ ) {
        auto it
          = random::sample( treelet_workers.begin(), treelet_workers.end() );
        workers_to_take_down.push_back( *it );
        treelet_workers.erase( it );
      }

      /* no workers are left for this treelet */
      if ( treelet_workers.empty() ) {
        unassigned_treelets.insert( tid );
        move_from_queued_to_pending( tid );
      }

      treelets[tid].pending_workers = 0;
    }
  }

  /* shuffling treeletsToSpawn */
  random_device rd {};
  mt19937 g { rd() };
  shuffle( treelets_to_spawn.begin(), treelets_to_spawn.end(), g );

  /* let's kill the workers we can kill */
  for ( const WorkerId worker_id : workers_to_take_down ) {
    auto& worker = workers.at( worker_id );
    worker.state = Worker::State::FinishingUp;
    worker.client.push_request( { 0, OpCode::FinishUp, "" } );
  }

  /* the rest will have to wait until we have available capacity */
}
