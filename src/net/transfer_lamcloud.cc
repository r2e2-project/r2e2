#include "transfer_lamcloud.hh"

#include <charconv>
#include <thread>

#include "messages/lamcloud/message.hh"
#include "util/split.hh"

using namespace std;
using namespace chrono;
using namespace lamcloud;

LamCloudTransferAgent::LamCloudTransferAgent( const uint16_t port )
  : TransferAgent()
  , _port( port )
{
  _thread_count = 1;

  for ( size_t i = 0; i < _thread_count; i++ ) {
    _threads.emplace_back( &LamCloudTransferAgent::worker_thread, this, i );
  }
}

LamCloudTransferAgent::~LamCloudTransferAgent()
{
  do_action( { 0, Task::Terminate, "", "" } );
  for ( auto& t : _threads ) {
    t.join();
  }
}

void LamCloudTransferAgent::do_action( Action&& action )
{
  {
    unique_lock<mutex> lock { _outstanding_mutex };
    _outstanding.push( move( action ) );
  }

  _action_event.write_event();
}

uint32_t get_remote_id( const string& key )
{
  vector<string_view> tokens;
  split( key, '/', tokens );
  string_view target_token = tokens.at( tokens.size() - 2 );
  target_token.remove_prefix( 2 );

  uint32_t id;
  if ( from_chars( target_token.begin(), target_token.end(), id ).ec
       != errc() ) {
    throw runtime_error( "could not find remote id in key" );
  }

  return id;
}

lamcloud::Message LamCloudTransferAgent::get_request( const Action& action )
{
  switch ( action.task ) {
    case Task::Upload: {
      string key = action.key;
      string val = action.data;

      Message msg { OpCode::LocalStore };
      msg.set_field( MessageField::Name, move( key ) );
      msg.set_field( MessageField::Object, move( val ) );
      return msg;
    }

    case Task::Download: {
      string key = action.key;
      string rid( 4, '\0' );
      const uint32_t remote_id = get_remote_id( action.key );
      memcpy( rid.data(), &remote_id, sizeof( remote_id ) );

      Message msg { OpCode::LocalRemoteLookup };
      msg.set_field( MessageField::Name, move( key ) );
      msg.set_field( MessageField::RemoteNode, move( rid ) );
      return msg;
    }

    case Task::Delete: {
      string key = action.key;

      Message msg { OpCode::LocalDelete };
      msg.set_field( MessageField::Name, move( key ) );
      return msg;
    }

    default:
      throw runtime_error( "Unknown action task" );
  }
}

#define TRY_OPERATION( x, y )                                                  \
  try {                                                                        \
    x;                                                                         \
  } catch ( exception & ex ) {                                                 \
    connection_okay = false;                                                   \
    sock.close();                                                              \
    y;                                                                         \
  }

void LamCloudTransferAgent::worker_thread( const size_t )
{
  using namespace std::placeholders;

  auto make_client = []( const Address& addr ) {
    TCPSocket socket;
    socket.set_blocking( false );
    socket.connect( addr );
    return make_unique<lamcloud::Client>( move( socket ) );
  };

  queue<pair<uint64_t, string>> thread_results;
  std::deque<Action> actions;

  queue<Action> pending_actions;
  unique_ptr<lamcloud::Client> client;

  lamcloud::Client::RuleCategories rule_categories {
    _loop.add_category( "TCP Session" ),
    _loop.add_category( "Client Read" ),
    _loop.add_category( "Client Write" ),
    _loop.add_category( "Response" )
  };

  auto response_callback
    = [&actions, &pending_actions, &thread_results]( Message&& response ) {
        switch ( response.opcode() ) {
          case OpCode::LocalStore:
            actions.emplace_front(
              0, Task::Delete, pending_actions.front().key, "" );

            [[fallthrough]];

          case OpCode::LocalSuccess:
            if ( pending_actions.front().task != Task::Delete ) {
              thread_results.emplace(
                pending_actions.front().id,
                move( response.get_field( MessageField::Object ) ) );
            }

            pending_actions.pop();
            break;

          case OpCode::LocalError:
            throw runtime_error( "client errored" );
            break;

          default:
            throw runtime_error( "invalid response" );
        }
      };

  client = make_client( { "", _port } );
  client->install_rules(
    _loop,
    rule_categories,
    [f = response_callback]( Message&& res ) { f( move( res ) ); },
    [] { throw runtime_error( "storage server died" ); },
    [] { throw runtime_error( "storage server died" ); } );

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
    // handle actions
    while ( not actions.empty() ) {
      auto& action = actions.front();

      switch ( action.task ) {
        case Task::Download:
        case Task::Upload:
        case Task::Delete:
          client->push_request( get_request( action ) );
          pending_actions.push( move( action ) );
          break;

        case Task::Terminate:
          return;

        default:
          throw runtime_error( "unsupported task" );
      }

      actions.pop_front();
    }
  }
}
