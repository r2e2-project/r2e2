#pragma once

#include <pbrt/main.h>
#include <pbrt/raystate.h>

#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include "lambda.hh"

#include "messages/message.hh"
#include "net/address.hh"
#include "net/client.hh"
#include "net/session.hh"
#include "net/socket.hh"
#include "net/transfer_s3.hh"
#include "util/eventloop.hh"
#include "util/exception.hh"
#include "util/temp_dir.hh"
#include "util/timerfd.hh"

#include "concurrentqueue/blockingconcurrentqueue.h"

namespace r2e2 {

class TileHelper
{
private:
  uint32_t accumulators_ { 0 };
  pbrt::Bounds2<uint32_t> bounds_ {};
  pbrt::Vector2<uint32_t> extent_ {};
  uint32_t spp_ {};

  uint32_t tile_size_ {};
  uint32_t active_accumulators_ {};

  pbrt::Vector2<uint32_t> n_tiles_ { 1, 1 };

public:
  TileHelper() = default;
  TileHelper( const TileHelper& ) = default;
  TileHelper& operator=( const TileHelper& ) = default;

  TileHelper( const uint32_t accumulators,
              const pbrt::Bounds2i& sample_bounds,
              const uint32_t spp );

  TileId tile_id( const pbrt::Sample& sample ) const;

  uint32_t tile_size() const { return tile_size_; }
  uint32_t active_accumulators() const { return active_accumulators_; }

  pbrt::Bounds2<uint32_t> bounds( const uint32_t tile_id ) const;
};

}
