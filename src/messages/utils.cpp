#include "utils.h"

#include <stdexcept>

namespace pbrt {

protobuf::Point2f to_protobuf(const Point2f & point) {
    protobuf::Point2f proto_point;
    proto_point.set_x(point.x);
    proto_point.set_y(point.y);
    return proto_point;
}

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

protobuf::TriangleMesh to_protobuf(const TriangleMesh& tm) {
    protobuf::TriangleMesh proto_tm;
    proto_tm.set_n_triangles(tm.nTriangles);
    proto_tm.set_n_vertices(tm.nVertices);

    for (size_t i = 0; i < tm.nTriangles; i++) {
        proto_tm.add_vertex_indices(tm.vertexIndices[3 * i]);
        proto_tm.add_vertex_indices(tm.vertexIndices[3 * i + 1]);
        proto_tm.add_vertex_indices(tm.vertexIndices[3 * i + 2]);
    }

    for (size_t i = 0; i < tm.nVertices; i++) {
        *proto_tm.add_p() = to_protobuf(tm.p[i]);
    }

    if (tm.uv != nullptr || tm.n != nullptr || tm.s != nullptr) {
        throw std::runtime_error("TriangleMesh: uv, n and s are not supported");
    }

    return proto_tm;
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

TriangleMesh from_protobuf(const protobuf::TriangleMesh &proto_tm) {
    Transform identity;
    std::vector<int> vertexIndices;
    std::vector<Point3f> p;

    vertexIndices.reserve(proto_tm.n_triangles() * 3);
    p.reserve(proto_tm.n_vertices());

    for (size_t i = 0; i < proto_tm.n_triangles(); i++) {
        vertexIndices.push_back(proto_tm.vertex_indices(3 * i));
        vertexIndices.push_back(proto_tm.vertex_indices(3 * i + 1));
        vertexIndices.push_back(proto_tm.vertex_indices(3 * i + 2));
    }

    for (size_t i = 0; i < proto_tm.n_vertices(); i++) {
        p.push_back(from_protobuf(proto_tm.p(i)));
    }

    return {identity, proto_tm.n_triangles(), vertexIndices.data(), proto_tm.n_vertices(),
            p.data(), nullptr, nullptr, nullptr, nullptr, nullptr, nullptr};
}

}
