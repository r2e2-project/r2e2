#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <future>
#include <iterator>
#include <limits>
#include <map>
#include <mutex>
#include <queue>
#include <string>

#include "net/address.hh"
#include "net/socket.hh"
#include "util/eventfd.hh"

class TransferAgent
{
public:
  enum class Task
  {
    Download,
    Upload,
    Delete,
    FlushAll,
    Terminate
  };

protected:
  struct Action
  {
    uint64_t id;
    Task task;
    std::string key;
    std::string data;

    Action( const uint64_t id,
            const Task task,
            const std::string& key,
            std::string&& data )
      : id( id )
      , task( task )
      , key( key )
      , data( move( data ) )
    {}
  };

  uint64_t _next_id { 1 };

  static constexpr size_t MAX_THREADS { 8 };
  size_t _thread_count { MAX_THREADS };

  std::vector<std::thread> _threads {};
  std::mutex _results_mutex {};
  std::mutex _outstanding_mutex {};
  std::condition_variable _cv {};

  std::queue<Action> _outstanding {};
  std::queue<std::pair<uint64_t, std::string>> _results {};

  EventFD _event_fd { false };

  virtual void do_action( Action&& action );
  virtual void worker_thread( const size_t threadId ) = 0;

public:
  TransferAgent() {}
  virtual ~TransferAgent();

  uint64_t request_download( const std::string& key );
  uint64_t request_upload( const std::string& key, std::string&& data );

  EventFD& eventfd() { return _event_fd; }

  bool empty() const;
  bool try_pop( std::pair<uint64_t, std::string>& output );

  template<class Container>
  size_t try_pop_bulk( std::back_insert_iterator<Container> insert_it,
                       const size_t max_count
                       = std::numeric_limits<size_t>::max() );
};

template<class Container>
size_t TransferAgent::try_pop_bulk(
  std::back_insert_iterator<Container> insert_it,
  const size_t max_count )
{
  std::unique_lock<std::mutex> lock { _results_mutex };

  if ( _results.empty() )
    return 0;

  size_t count;
  for ( count = 0; !_results.empty() && count < max_count; count++ ) {
    insert_it = std::move( _results.front() );
    _results.pop();
  }

  return count;
}
