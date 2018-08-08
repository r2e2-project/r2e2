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

protobuf::Matrix to_protobuf(const Matrix4x4 & matrix) {
    protobuf::Matrix proto_matrix;
    for(size_t i = 0; i < 4; i++) {
        for (size_t j = 0; j < 4; j++) {
            proto_matrix.add_m(matrix.m[i][j]);
        }
    }
    return proto_matrix;
}

protobuf::AnimatedTransform to_protobuf(const AnimatedTransform & transform) {
    protobuf::AnimatedTransform proto_transform;
    *proto_transform.mutable_start_transform() = to_protobuf(transform.startTransform->GetMatrix());
    *proto_transform.mutable_end_transform() = to_protobuf(transform.endTransform->GetMatrix());
    proto_transform.set_start_time(transform.startTime);
    proto_transform.set_end_time(transform.endTime);
    return proto_transform;
}

Point3f from_protobuf(const protobuf::Point3f & point) {
    return {point.x(), point.y(), point.z()};
}

Bounds3f from_protobuf(const protobuf::Bounds3f & bounds) {
    return {from_protobuf(bounds.point_min()), from_protobuf(bounds.point_max())};
}

Matrix4x4 from_protobuf(const protobuf::Matrix & proto_matrix) {
    Matrix4x4 matrix;
    for(size_t i = 0; i < 4; i++) {
        for (size_t j = 0; j < 4 and (4 * i + j < proto_matrix.m_size()); j++) {
            matrix.m[i][j] = proto_matrix.m(4 * i + j);
        }
    }
    return matrix;
}

}
