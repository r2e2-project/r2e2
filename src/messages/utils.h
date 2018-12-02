#ifndef PBRT_MESSAGES_UTILS_H
#define PBRT_MESSAGES_UTILS_H

#include "geometry.h"
#include "pbrt.pb.h"
#include "shapes/triangle.h"
#include "transform.h"

namespace pbrt {

protobuf::Point2f to_protobuf(const Point2f& point);
protobuf::Point3f to_protobuf(const Point3f& point);
protobuf::Vector3f to_protobuf(const Vector3f& point);
protobuf::Normal3f to_protobuf(const Normal3f& point);
protobuf::Bounds3f to_protobuf(const Bounds3f& bounds);
protobuf::Matrix to_protobuf(const Matrix4x4& matrix);
protobuf::AnimatedTransform to_protobuf(const AnimatedTransform& transform);
protobuf::TriangleMesh to_protobuf(const TriangleMesh& triangleMesh);

Point2f from_protobuf(const protobuf::Point2f& point);
Point3f from_protobuf(const protobuf::Point3f& point);
Vector3f from_protobuf(const protobuf::Vector3f& point);
Normal3f from_protobuf(const protobuf::Normal3f& point);
Bounds3f from_protobuf(const protobuf::Bounds3f& bounds);
Matrix4x4 from_protobuf(const protobuf::Matrix& matrix);
TriangleMesh from_protobuf(const protobuf::TriangleMesh& mesh);

}  // namespace pbrt

#endif /* PBRT_MESSAGES_UTILS_H */
