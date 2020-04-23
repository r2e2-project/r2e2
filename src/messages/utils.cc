#include "utils.hh"

#include <chrono>
#include <memory>
#include <stdexcept>

#include "util/util.hh"

using namespace std;

namespace r2t2 {

protobuf::SceneObject to_protobuf( const SceneObject& object )
{
  protobuf::SceneObject proto;
  proto.set_type( to_underlying( object.key.type ) );
  proto.set_id( object.key.id );
  proto.set_alt_name( object.alt_name );
  return proto;
}

protobuf::RayBagInfo to_protobuf( const RayBagInfo& info )
{
  protobuf::RayBagInfo proto;
  proto.set_tracked( info.tracked );
  proto.set_worker_id( info.worker_id );
  proto.set_treelet_id( info.treelet_id );
  proto.set_bag_id( info.bag_id );
  proto.set_ray_count( info.ray_count );
  proto.set_bag_size( info.bag_size );
  proto.set_sample_bag( info.sample_bag );
  return proto;
}

protobuf::WorkerStats to_protobuf( const WorkerStats& stats )
{
  protobuf::WorkerStats proto;
  proto.set_finished_paths( stats.finishedPaths );
  proto.set_cpu_usage( stats.cpuUsage );
  return proto;
}

SceneObject from_protobuf( const protobuf::SceneObject& object )
{
  return { { static_cast<pbrt::ObjectType>( object.type() ), object.id() },
           object.alt_name() };
}

RayBagInfo from_protobuf( const protobuf::RayBagInfo& proto )
{
  RayBagInfo res { proto.worker_id(), proto.treelet_id(), proto.bag_id(),
                   proto.ray_count(), proto.bag_size(),   proto.sample_bag() };

  res.tracked = proto.tracked();
  return res;
}

WorkerStats from_protobuf( const protobuf::WorkerStats& proto )
{
  return { proto.finished_paths(), proto.cpu_usage() };
}

} // namespace r2t2
