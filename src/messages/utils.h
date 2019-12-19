#ifndef PBRT_MESSAGES_UTILS_H
#define PBRT_MESSAGES_UTILS_H

#include <google/protobuf/util/json_util.h>

#include "cloud/integrator.h"
#include "cloud/manager.h"
#include "cloud/stats.h"
#include "core/geometry.h"
#include "core/light.h"
#include "core/paramset.h"
#include "core/sampler.h"
#include "core/spectrum.h"
#include "core/transform.h"

#include "pbrt.pb.h"
#include "shapes/triangle.h"

namespace protoutil {

template <class ProtobufType>
std::string to_string(const ProtobufType& proto) {
    return proto.SerializeAsString();
}

template <class ProtobufType>
void from_string(const std::string& data, ProtobufType& dest) {
    dest.ParseFromString(data);
}

template <class ProtobufType>
std::string to_json(const ProtobufType& protobuf,
                    const bool pretty_print = false) {
    using namespace google::protobuf::util;
    JsonPrintOptions print_options;
    print_options.add_whitespace = pretty_print;
    print_options.always_print_primitive_fields = true;

    std::string ret;
    if (not MessageToJsonString(protobuf, &ret, print_options).ok()) {
        throw std::runtime_error("cannot convert protobuf to json");
    }

    return ret;
}

}  // namespace protoutil

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
protobuf::SampleData to_protobuf(const CloudIntegrator::SampleData& sample);
protobuf::ParamSet to_protobuf(const ParamSet& paramset);
protobuf::Scene to_protobuf(const Scene& scene);
protobuf::TextureParams to_protobuf(const TextureParams& texture_params);
protobuf::ObjectKey to_protobuf(const ObjectKey& ObjectKey);
protobuf::RayStats to_protobuf(const RayStats& stats);
protobuf::QueueStats to_protobuf(const QueueStats& stats);
protobuf::NetStats to_protobuf(const NetStats& stats);
protobuf::WorkerStats to_protobuf(const WorkerStats& stats);
protobuf::WorkerDiagnostics to_protobuf(const WorkerDiagnostics& diagnostics);
protobuf::FinishedRay to_protobuf(const FinishedRay& finishedRay);
protobuf::RayBagKey to_protobuf(const RayBagKey& rayBagKey);

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
CloudIntegrator::SampleData from_protobuf(const protobuf::SampleData& sample);
ParamSet from_protobuf(const protobuf::ParamSet& paramset);
Scene from_protobuf(const protobuf::Scene& scene);
TextureParams from_protobuf(
    const protobuf::TextureParams& texture_params, ParamSet& geom_params,
    ParamSet& material_params,
    std::map<std::string, std::shared_ptr<Texture<Float>>>& fTex,
    std::map<std::string, std::shared_ptr<Texture<Spectrum>>>& sTex);
ObjectKey from_protobuf(const protobuf::ObjectKey& ObjectKey);
RayStats from_protobuf(const protobuf::RayStats& state);
WorkerStats from_protobuf(const protobuf::WorkerStats& state);
WorkerDiagnostics from_protobuf(const protobuf::WorkerDiagnostics& diagnostics);
FinishedRay from_protobuf(const protobuf::FinishedRay& finishedRay);
RayBagKey from_protobuf(const protobuf::RayBagKey& rayBagKey);

namespace light {

std::shared_ptr<Light> from_protobuf(const protobuf::Light& light);
protobuf::Light to_protobuf(const std::string& name, const ParamSet& params,
                            const Transform& light2world);

}  // namespace light

namespace sampler {

std::shared_ptr<GlobalSampler> from_protobuf(const protobuf::Sampler& sampler,
                                             const int samplesPerPixel = 0);
protobuf::Sampler to_protobuf(const std::string& name, const ParamSet& params,
                              const Bounds2i& sampleBounds);

}  // namespace sampler

namespace camera {

std::shared_ptr<Camera> from_protobuf(
    const protobuf::Camera& camera,
    std::vector<std::unique_ptr<Transform>>& transformCache);

protobuf::Camera to_protobuf(const std::string& name, const ParamSet& params,
                             const AnimatedTransform& cam2world,
                             const std::string& filmName,
                             const ParamSet& filmParams,
                             const std::string& filterName,
                             const ParamSet& filterParams);

}  // namespace camera

namespace material {

std::shared_ptr<Material> from_protobuf(const protobuf::Material& material);

protobuf::Material to_protobuf(const std::string& name,
                               const TextureParams& tp);

}  // namespace material

namespace float_texture {

std::shared_ptr<Texture<Float>> from_protobuf(
    const protobuf::FloatTexture& texture);

protobuf::FloatTexture to_protobuf(const std::string& name,
                                   const Transform& tex2world,
                                   const TextureParams& tp);

}  // namespace float_texture

namespace spectrum_texture {

std::shared_ptr<Texture<Spectrum>> from_protobuf(
    const protobuf::SpectrumTexture& texture);

protobuf::SpectrumTexture to_protobuf(const std::string& name,
                                      const Transform& tex2world,
                                      const TextureParams& tp);

}  // namespace spectrum_texture

}  // namespace pbrt

#endif /* PBRT_MESSAGES_UTILS_H */
