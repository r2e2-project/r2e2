/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#pragma once

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
