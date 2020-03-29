/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#pragma once

#include <chrono>
#include <functional>

template<class TimeUnit>
TimeUnit time_it( const std::function<void()>& f );
