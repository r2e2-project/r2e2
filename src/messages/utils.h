#ifndef PBRT_MESSAGES_UTILS_H
#define PBRT_MESSAGES_UTILS_H

#include "cloud/raystate.h"
#include "core/geometry.h"
#include "core/light.h"
#include "core/paramset.h"
#include "core/sampler.h"
#include "core/spectrum.h"
#include "core/transform.h"

#include "pbrt.pb.h"
#include "shapes/triangle.h"

namespace pbrt {

protobuf::Point2i to_protobuf(const Point2i& point);
protobuf::Point2f to_protobuf(const Point2f& point);
protobuf::Point3f to_protobuf(const Point3f& point);
protobuf::Vector2f to_protobuf(const Vector2f& point);
protobuf::Vector3f to_protobuf(const Vector3f& point);
protobuf::Normal3f to_protobuf(const Normal3f& point);
protobuf::Bounds2i to_protobuf(const Bounds2i& bounds);
protobuf::Bounds2f to_protobuf(const Bounds2f& bounds);
protobuf::Bounds3f to_protobuf(const Bounds3f& bounds);
protobuf::Matrix to_protobuf(const Matrix4x4& matrix);
protobuf::RGBSpectrum to_protobuf(const RGBSpectrum& spectrum);
protobuf::RayDifferential to_protobuf(const RayDifferential& ray);
protobuf::AnimatedTransform to_protobuf(const AnimatedTransform& transform);
protobuf::TriangleMesh to_protobuf(const TriangleMesh& triangleMesh);
protobuf::VisitNode to_protobuf(const RayState::TreeletNode& node);
protobuf::RayState to_protobuf(const RayState& state);
protobuf::ParamSet to_protobuf(const ParamSet& paramset);

Point2i from_protobuf(const protobuf::Point2i& point);
Point2f from_protobuf(const protobuf::Point2f& point);
Point3f from_protobuf(const protobuf::Point3f& point);
Vector2f from_protobuf(const protobuf::Vector2f& point);
Vector3f from_protobuf(const protobuf::Vector3f& point);
Normal3f from_protobuf(const protobuf::Normal3f& point);
Bounds2i from_protobuf(const protobuf::Bounds2i& bounds);
Bounds2f from_protobuf(const protobuf::Bounds2f& bounds);
Bounds3f from_protobuf(const protobuf::Bounds3f& bounds);
Matrix4x4 from_protobuf(const protobuf::Matrix& matrix);
RGBSpectrum from_protobuf(const protobuf::RGBSpectrum& spectrum);
RayDifferential from_protobuf(const protobuf::RayDifferential& ray);
TriangleMesh from_protobuf(const protobuf::TriangleMesh& mesh);
RayState::TreeletNode from_protobuf(const protobuf::VisitNode& node);
RayState from_protobuf(const protobuf::RayState& state);
ParamSet from_protobuf(const protobuf::ParamSet& paramset);

namespace light {

protobuf::Light to_protobuf(const std::string& name, const ParamSet& params,
                            const Transform& light2world);

std::shared_ptr<Light> from_protobuf(const protobuf::Light& light);

}  // namespace light

namespace sampler {

protobuf::Sampler to_protobuf(const std::string& name, const ParamSet& params,
                              const Bounds2i& sampleBounds);

std::shared_ptr<Sampler> from_protobuf(const protobuf::Sampler& sampler);

}  // namespace sampler

}  // namespace pbrt

#endif /* PBRT_MESSAGES_UTILS_H */
