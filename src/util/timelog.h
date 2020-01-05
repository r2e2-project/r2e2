#pragma once

#include <array>
#include <chrono>
#include <string>
#include <type_traits>

#include "util/optional.h"

inline uint64_t timestamp_ns()
{
  static_assert( std::is_same<std::chrono::steady_clock::duration, std::chrono::nanoseconds>::value );

  return std::chrono::steady_clock::now().time_since_epoch().count();
}

class TimeLog
{
public:
  enum class Category
  {
    DNS,
    Nonblock,
    WaitingForEvent,
    count
  };

  constexpr static size_t num_categories = static_cast<size_t>( Category::count );

  constexpr static std::array<const char*, num_categories> _category_names {
    { "DNS", "Nonblocking operations", "Waiting for event" }
  };

private:
  uint64_t _beginning_timestamp = timestamp_ns();

  struct Record
  {
    uint64_t count;
    uint64_t total_ns;
    uint64_t max_ns;
  };

  std::array<Record, num_categories> _logs {};

public:
  void log( const Category category, const uint64_t time_ns )
  {
    auto& entry = _logs[static_cast<size_t>( category )];

    entry.count++;
    entry.total_ns += time_ns;
    entry.max_ns = std::max( entry.max_ns, time_ns );
  }

  std::string summary() const;
};

class Timer
{
private:
  TimeLog _log {};
  Optional<TimeLog::Category> _current_category {};
  uint64_t _start_time {};

public:
  template<TimeLog::Category category>
  void start()
  {
    if ( _current_category.initialized() ) {
      throw std::runtime_error( "timer started when already running" );
    }

    _current_category = category;
    _start_time = timestamp_ns();
  }

  template<TimeLog::Category category>
  void stop()
  {
    if ( not _current_category.initialized() or *_current_category != category ) {
      throw std::runtime_error( "timer stopped when not running, or with mismatched category" );
    }

    _log.log( category, timestamp_ns() - _start_time );
    _current_category.clear();
  }

  std::string summary() const { return _log.summary(); }
};

inline Timer& timer()
{
  static Timer the_global_timer;
  return the_global_timer;
}

template<TimeLog::Category category>
class ScopeTimer
{
public:
  ScopeTimer() { timer().start<category>(); }
  ~ScopeTimer() { timer().stop<category>(); }
};
