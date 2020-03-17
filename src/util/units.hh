/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#ifndef PBRT_UTIL_UNITS_HH
#define PBRT_UTIL_UNITS_HH

#include <cstddef>

constexpr std::size_t operator"" _kbps( unsigned long long v )
{
  return 1000u * v;
}

constexpr std::size_t operator"" _Mbps( unsigned long long v )
{
  return 1000_kbps * v;
}

constexpr std::size_t operator"" _KiB( unsigned long long v )
{
  return 1024u * v;
}

constexpr std::size_t operator"" _MiB( unsigned long long v )
{
  return 1024_KiB * v;
}

#endif /* PBRT_UTIL_UNITS_HH */
