#pragma once

#include <iterator>
#include <random>

namespace r2e2::random {

template<typename Iter, typename RandomGenerator>
Iter sample( Iter start, Iter end, RandomGenerator& g )
{
  std::uniform_int_distribution<> dis( 0, std::distance( start, end ) - 1 );
  std::advance( start, dis( g ) );
  return start;
}

template<typename Iter>
Iter sample( Iter start, Iter end )
{
  static thread_local std::random_device rd;
  static thread_local std::mt19937 gen( rd() );
  return sample( start, end, gen );
}

} // namespace r2e2::random
