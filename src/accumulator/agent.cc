#include "accumulator.hh"

#include <algorithm>
#include <cmath>
#include <memory>

using namespace std;

namespace r2t2 {

TileHelper::TileHelper( const uint32_t accumulators,
                        const pbrt::Bounds2i& sample_bounds,
                        const uint32_t spp )
  : accumulators_( accumulators )
  , bounds_( sample_bounds )
  , extent_( bounds_.Diagonal() )
  , spp_( spp )
{
  tile_size_ = ceil( sqrt( extent_.x * extent_.y / accumulators_ ) );

  while ( ceil( 1.0 * extent_.x / tile_size_ )
            * ceil( 1.0 * extent_.y / tile_size_ )
          > accumulators_ ) {
    tile_size_++;
  }

  active_accumulators_
    = static_cast<uint32_t>( ceil( 1.0 * extent_.x / tile_size_ )
                             * ceil( 1.0 * extent_.y / tile_size_ ) );

  n_tiles_ = { ( extent_.x + tile_size_ - 1 ) / tile_size_,
               ( extent_.y + tile_size_ - 1 ) / tile_size_ };
}

TileId TileHelper::tile_id( const pbrt::Sample& sample ) const
{
  if ( active_accumulators_ <= 1 )
    return 0;

  const auto pixel = sample.SamplePixel(
    { static_cast<int>( extent_.x ), static_cast<int>( extent_.y ) }, spp_ );
  return pixel.x / tile_size_ + pixel.y / tile_size_ * n_tiles_.x;
}

pbrt::Bounds2<uint32_t> TileHelper::bounds( const uint32_t tile_id ) const
{
  if ( tile_id >= active_accumulators_ ) {
    throw runtime_error( "empty bounds" );
  }

  const uint32_t tile_x = tile_id % n_tiles_.x;
  const uint32_t tile_y = tile_id / n_tiles_.x;

  const uint32_t x0 = bounds_.pMin.x + tile_x * tile_size_;
  const uint32_t x1 = min( x0 + tile_size_, bounds_.pMax.x );
  const uint32_t y0 = bounds_.pMin.y + tile_y * tile_size_;
  const uint32_t y1 = min( y0 + tile_size_, bounds_.pMax.y );

  return { { x0, y0 }, { x1, y1 } };
}

AccumulatorTransferAgent::AccumulatorTransferAgent(
  const std::vector<Address>& accumulators )
  : _accumulators( accumulators )
{
  if ( _accumulators.empty() ) {
    throw runtime_error( "no accumulator specified" );
  }

  _thread_count = 1;
  _threads.emplace_back( &AccumulatorTransferAgent::worker_thread, this, 0 );

  _loop.set_fd_failure_callback( [] {} );
}

AccumulatorTransferAgent::~AccumulatorTransferAgent()
{
  do_action( { 0, Task::Terminate, "", "" } );
  for ( auto& t : _threads ) {
    t.join();
  }
}

void AccumulatorTransferAgent::do_action( Action&& action )
{
  {
    unique_lock<mutex> lock { _outstanding_mutex };
    _outstanding.push( move( action ) );
  }

  _action_event.write_event();
}

void AccumulatorTransferAgent::worker_thread( const size_t )
{
  using namespace std::placeholders;
  using Client = meow::Client<TCPSession>;
  using OpCode = meow::Message::OpCode;

  auto make_client = []( const Address& addr ) {
    TCPSocket socket;
    socket.set_blocking( false );
    socket.connect( addr );
    return make_unique<Client>( move( socket ) );
  };

  queue<pair<uint64_t, string>> thread_results;

  std::deque<Action> actions;

  auto pending_actions = make_unique<queue<Action>[]>( _accumulators.size() );
  auto clients = make_unique<unique_ptr<Client>[]>( _accumulators.size() );

  queue<size_t> dead_clients {};

  Client::RuleCategories rule_categories { _loop.add_category( "TCP Session" ),
                                           _loop.add_category( "Client Read" ),
                                           _loop.add_category( "Client Write" ),
                                           _loop.add_category( "Response" ) };

  auto response_callback = [&actions, &pending_actions, &thread_results](
                             const size_t i, meow::Message&& response ) {
    switch ( response.opcode() ) {
      case OpCode::SampleBagProcessed:
        thread_results.emplace( pending_actions[i].front().id,
                                move( response.payload() ) );
        pending_actions[i].pop();
        break;

      default:
        throw runtime_error( "invalid response: " + response.info() );
    }
  };

  auto cancel_callback
    = [&dead_clients]( const size_t i ) { dead_clients.push( i ); };

  for ( size_t i = 0; i < _accumulators.size(); i++ ) {
    const auto& server = _accumulators[i];

    clients[i] = make_client( server );
    clients[i]->install_rules(
      _loop,
      rule_categories,
      [f = response_callback, i]( meow::Message&& r ) { f( i, move( r ) ); },
      [f = cancel_callback, i] { f( i ); },
      [f = cancel_callback, i] { f( i ); } );
  }

  _loop.add_rule(
    "New actions",
    Direction::In,
    _action_event,
    [&] {
      if ( not _action_event.read_event() ) {
        return;
      }

      unique_lock<mutex> lock { _outstanding_mutex };

      while ( not _outstanding.empty() ) {
        actions.push_back( move( _outstanding.front() ) );
        _outstanding.pop();
      }
    },
    [] { return true; } );

  _loop.add_rule(
    "Push results",
    [&] {
      {
        unique_lock<mutex> lock { _results_mutex };
        while ( !thread_results.empty() ) {
          _results.push( move( thread_results.front() ) );
          thread_results.pop();
        }
      }

      _event_fd.write_event();
    },
    [&thread_results] { return !thread_results.empty(); } );

  while ( _loop.wait_next_event( -1 ) != EventLoop::Result::Exit ) {
    // reconnect dead clients
    while ( not dead_clients.empty() ) {
      const auto i = dead_clients.front();
      dead_clients.pop();

      clients[i] = make_client( _accumulators[i] );
      clients[i]->install_rules(
        _loop,
        rule_categories,
        [f = response_callback, i]( meow::Message&& r ) { f( i, move( r ) ); },
        [f = cancel_callback, i] { f( i ); },
        [f = cancel_callback, i] { f( i ); } );

      while ( not pending_actions[i].empty() ) {
        actions.emplace_back( move( pending_actions[i].front() ) );
        pending_actions[i].pop();
      }
    }

    // handle actions
    while ( not actions.empty() ) {
      auto& action = actions.front();
      const size_t server_id = action.helper_data;

      switch ( action.task ) {
        case Task::Download:
          throw runtime_error( "Download task not supported" );
          break;

        case Task::Upload:
          clients[server_id]->push_request(
            { 0, OpCode::ProcessSampleBag, move( action.data ) } );
          pending_actions[server_id].push( move( action ) );
          break;

        case Task::FlushAll:
          break;

        case Task::Delete:
          throw runtime_error( "Delete task not supported" );
          break;

        case Task::Terminate:
          return;
      }

      actions.pop_front();
    }
  }
}

}
