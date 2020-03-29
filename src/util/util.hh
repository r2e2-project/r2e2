/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#pragma once

#include <sys/time.h>

#include <chrono>
#include <string>

std::string safe_getenv( const std::string& key );
std::string safe_getenv_or( const std::string& key,
                            const std::string& def_val );
std::string format_bytes( size_t bytes );
std::string format_num( size_t num );

template<typename E>
constexpr auto to_underlying( E e ) noexcept
{
  return static_cast<std::underlying_type_t<E>>( e );
}

inline std::string pluralize( const std::string& word, const size_t count )
{
  return word + ( count != 1 ? "s" : "" );
}

template<typename Duration>
inline void to_timespec( const Duration& d, timespec& tv )
{
  const auto sec = std::chrono::duration_cast<std::chrono::seconds>( d );

  tv.tv_sec = sec.count();
  tv.tv_nsec
    = std::chrono::duration_cast<std::chrono::nanoseconds>( d - sec ).count();
}

template<typename Duration>
inline void to_timeval( const Duration& d, timeval& tv )
{
  const auto sec = std::chrono::duration_cast<std::chrono::seconds>( d );

  tv.tv_sec = sec.count();
  tv.tv_usec
    = std::chrono::duration_cast<std::chrono::microseconds>( d - sec ).count();
}
