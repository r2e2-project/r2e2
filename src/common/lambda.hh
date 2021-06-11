#pragma once

#include <chrono>
#include <cstdint>
#include <sstream>
#include <string>

#include <pbrt/main.h>

#include "util/uri.hh"

constexpr std::chrono::milliseconds DEFAULT_BAGGING_DELAY { 50 };
constexpr size_t WORKER_MAX_ACTIVE_RAYS = 100'000;       /* ~120 MiB of rays */
constexpr size_t WORKER_MAX_ACTIVE_SAMPLES = 10'000'000; /* 320 MB of samples */

using WorkerId = uint64_t;
using TreeletId = uint32_t;
using BagId = uint64_t;
using TileId = TreeletId;

struct Storage
{
  std::string region {};
  std::string bucket {};
  std::string path {};

  Storage( const std::string& uri )
  {
    ParsedURI u { uri };
    region = u.options.count( "region" ) ? u.options["region"] : "us-east-1";
    bucket = u.host;
    path = u.path;
  }
};

struct SceneObject
{
  pbrt::ObjectKey key;
  std::string alt_name {};

  SceneObject( const pbrt::ObjectKey& key_ )
    : key( key_ )
  {}

  SceneObject( const pbrt::ObjectKey& key_, const std::string& name )
    : key( key_ )
    , alt_name( name )
  {}

  bool operator<( const SceneObject& other ) const
  {
    if ( key < other.key ) {
      return true;
    } else if ( other.key < key ) {
      return false;
    } else {
      return alt_name < other.alt_name;
    }
  }
};

struct RayBagInfo
{
  bool tracked { false };

  WorkerId worker_id {};

  union
  {
    TreeletId treelet_id {};
    TileId tile_id;
  };

  BagId bag_id {};
  size_t ray_count {};
  size_t bag_size {};
  bool sample_bag { false };

  std::string str( const std::string& prefix ) const
  {
    std::ostringstream oss;

    if ( !sample_bag ) {
      oss << prefix << "T" << treelet_id << "/W" << worker_id << "/B" << bag_id;
    } else {
      oss << prefix << "samples/T" << tile_id << "/W" << worker_id << "/B"
          << bag_id;
    }

    return oss.str();
  }

  RayBagInfo( const WorkerId worker_id_,
              const TreeletId treelet_id_,
              const BagId bag_id_,
              const size_t ray_count_,
              const size_t bag_size_,
              const bool sample_bag_ )
    : worker_id( worker_id_ )
    , treelet_id( treelet_id_ )
    , bag_id( bag_id_ )
    , ray_count( ray_count_ )
    , bag_size( bag_size_ )
    , sample_bag( sample_bag_ )
  {}

  RayBagInfo() = default;
  RayBagInfo( const RayBagInfo& ) = default;
  RayBagInfo& operator=( const RayBagInfo& ) = default;

  bool operator<( const RayBagInfo& other ) const
  {
    return ( worker_id < other.worker_id )
           or ( worker_id == other.worker_id
                and ( treelet_id < other.treelet_id
                      or ( treelet_id == other.treelet_id
                           and bag_id < other.bag_id ) ) );
  }

  static RayBagInfo& EmptyBag()
  {
    static RayBagInfo bag;
    return bag;
  }
};

struct RayBag
{
  std::chrono::steady_clock::time_point created_at {
    std::chrono::steady_clock::now()
  };

  RayBagInfo info;
  std::string data;

  RayBag( const WorkerId worker_id,
          const TreeletId treelet_id,
          const BagId bag_id,
          const bool finished,
          const size_t max_bag_len )
    : info( worker_id, treelet_id, bag_id, 0, 0, finished )
    , data( max_bag_len, '\0' )
  {}

  RayBag( const RayBagInfo& info_, std::string&& data_ )
    : info( info_ )
    , data( std::move( data_ ) )
  {}
};
