#include "utils.h"

#include <chrono>
#include <memory>
#include <stdexcept>

#include "util/util.h"

using namespace std;

namespace r2t2 {

protobuf::ObjectKey to_protobuf(const pbrt::ObjectKey& key) {
    protobuf::ObjectKey proto;
    proto.set_type(to_underlying(key.type));
    proto.set_id(key.id);
    return proto;
}

protobuf::RayBagInfo to_protobuf(const RayBagInfo& info) {
    protobuf::RayBagInfo proto;
    proto.set_tracked(info.tracked);
    proto.set_worker_id(info.workerId);
    proto.set_treelet_id(info.treeletId);
    proto.set_bag_id(info.bagId);
    proto.set_ray_count(info.rayCount);
    proto.set_bag_size(info.bagSize);
    proto.set_sample_bag(info.sampleBag);
    return proto;
}

protobuf::WorkerStats to_protobuf(const WorkerStats& stats) {
    protobuf::WorkerStats proto;
    proto.set_finished_paths(stats.finishedPaths);
    proto.set_cpu_usage(stats.cpuUsage);
    return proto;
}

pbrt::ObjectKey from_protobuf(const protobuf::ObjectKey& key) {
    return pbrt::ObjectKey{static_cast<pbrt::ObjectType>(key.type()), key.id()};
}

RayBagInfo from_protobuf(const protobuf::RayBagInfo& proto) {
    RayBagInfo res{proto.worker_id(), proto.treelet_id(), proto.bag_id(),
                   proto.ray_count(), proto.bag_size(),   proto.sample_bag()};

    res.tracked = proto.tracked();
    return res;
}

WorkerStats from_protobuf(const protobuf::WorkerStats& proto) {
    return {proto.finished_paths(), proto.cpu_usage()};
}

}  // namespace r2t2
