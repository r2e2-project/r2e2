#include "utils.h"

namespace pbrt {

protobuf::Point3f to_protobuf(const Point3f & point) {
    protobuf::Point3f proto_point;
    proto_point.set_x(point.x);
    proto_point.set_y(point.y);
    proto_point.set_z(point.z);
    return proto_point;
}

protobuf::Bounds3f to_protobuf(const Bounds3f & bounds) {
    protobuf::Bounds3f proto_bounds;
    *proto_bounds.mutable_point_min() = to_protobuf(bounds.pMin);
    *proto_bounds.mutable_point_max() = to_protobuf(bounds.pMax);
    return proto_bounds;
}

Point3f from_protobuf(const protobuf::Point3f & point) {
    return {point.x(), point.y(), point.z()};
}

Bounds3f from_protobuf(const protobuf::Bounds3f & bounds) {
    return {from_protobuf(bounds.point_min()), from_protobuf(bounds.point_max())};
}

}
