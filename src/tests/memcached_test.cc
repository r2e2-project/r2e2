#include <chrono>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <map>
#include <random>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "transfer/memcached.hh"
#include "util/file_descriptor.hh"
#include "util/timerfd.hh"

using namespace std;
using namespace chrono_literals;

string get_random_key( const size_t length )
{
  auto randchar = [] {
    const char charset[] = "0123456789"
                           "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                           "abcdefghijklmnopqrstuvwxyz";
    const size_t max_index = ( sizeof( charset ) - 1 );
    return charset[rand() % max_index];
  };

  string str( length, 0 );
  generate_n( str.begin(), length, randchar );
  return str;
}

string get_random_blob( const size_t min_size, const size_t max_size )
{
  static mt19937 gen { random_device {}() };
  const auto size
    = uniform_int_distribution<size_t> { min_size, max_size }( gen );
  string result( size, '\0' );

  FileDescriptor fd { open( "/dev/random", O_RDONLY ) };
  fd.read( { result } );

  return result;
}

size_t get_hash( const string& key )
{
  size_t result = 5381;
  for ( const char c : key )
    result = ( ( result << 5 ) + result ) + c;
  return result;
}

int main( int argc, char* argv[] )
{
  if ( argc != 4 ) {
    cerr << "memcached_test <IP> <PORT> <T>" << endl;
  }

  srand( time( nullptr ) );

  const string ip { argv[1] };
  const uint16_t port = static_cast<uint16_t>( stoul( argv[2] ) );
  const size_t total_time = stoul( argv[3] );

  enum Action
  {
    PUT,
    GET
  };

  const Address memcached_server { ip, port };
  memcached::TransferAgent transfer_agent { { memcached_server } };
  map<uint64_t, pair<Action, string>> pending_actions;

  bool terminated = false;

  EventLoop loop;
  TimerFD action_timer { 1ms };
  TimerFD termination_timer { chrono::seconds { total_time } };

  uint64_t object_id = 0;
  map<string, size_t> objects;
  set<string> active_objects;

  size_t put_count = 0;
  size_t get_count = 0;

  const auto start = chrono::steady_clock::now();

  loop.add_rule(
    "Terminate",
    Direction::In,
    termination_timer,
    [&] {
      termination_timer.read_event();
      terminated = true;
    },
    [&] { return not terminated; } );

  loop.add_rule(
    "Action timer",
    Direction::In,
    action_timer,
    [&] {
      static mt19937 gen { random_device {}() };
      static uniform_int_distribution<size_t> count_distrib { 1, 25 };

      action_timer.read_event();

      const auto seconds_passed
        = min( static_cast<size_t>( chrono::duration_cast<chrono::seconds>(
                                      chrono::steady_clock::now() - start )
                                      .count() ),
               total_time );

      // more puts in the beginning, and then more gets at the end
      bernoulli_distribution coin { 1 - 1.f * seconds_passed / total_time };

      const auto action_count = count_distrib( gen );
      const auto action_get = not coin( gen );

      for ( size_t i = 0; i < action_count
                          and not( action_get and not active_objects.empty() );
            i++ ) {
        if ( not action_get ) {
          put_count++;

          // let's PUT
          const string key = get_random_key( 30 ) + to_string( object_id++ );
          string random_blob = get_random_blob( 500, 1024 * 1024 );
          const auto blob_hash = get_hash( random_blob );

          const auto action_id
            = transfer_agent.request_upload( key, move( random_blob ) );

          objects[key] = blob_hash;
          active_objects.insert( key );
          pending_actions[action_id] = make_pair( PUT, key );
        } else {
          get_count++;

          // let's GET a random object
          auto it = active_objects.begin();
          advance( it,
                   uniform_int_distribution<size_t> {
                     0u, active_objects.size() - 1 }( gen ) );

          const auto action_id = transfer_agent.request_download( *it );
          pending_actions[action_id] = make_pair( GET, *it );
          active_objects.erase( it );
        }
      }
    },
    [&] { return true; } );

  loop.add_rule(
    "Transfer agent",
    Direction::In,
    transfer_agent.eventfd(),
    [&] {
      transfer_agent.eventfd().read_event();

      vector<pair<uint64_t, string>> actions;
      transfer_agent.try_pop_bulk( back_inserter( actions ) );

      for ( auto& action : actions ) {
        auto& our_record = pending_actions.at( action.first );

        if ( our_record.first == GET ) {
          const auto& key = our_record.second;
          const auto ret_hash = get_hash( action.second );
          const auto our_hash = objects.at( key );
          objects.erase( key );

          if ( our_hash != ret_hash ) {
            cerr << "hash mistmach: " << ret_hash << " vs. " << our_hash
                 << endl;
            exit( 1 );
          }
        }

        pending_actions.erase( action.first );
      }
    },
    [&] { return true; } );

  while ( not terminated
          and loop.wait_next_event( -1 ) != EventLoop::Result::Exit ) {
  }

  cout << "PUT: " << put_count << endl << "GET: " << get_count << endl;

  return EXIT_SUCCESS;
}
