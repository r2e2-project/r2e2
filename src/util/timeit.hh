/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#ifndef PBRT_UTIL_TIMEIT_HH
#define PBRT_UTIL_TIMEIT_HH

#include <functional>
#include <chrono>

template<class TimeUnit>
TimeUnit time_it( const std::function<void()> & f );

#endif /* PBRT_UTIL_TIMEIT_HH */
