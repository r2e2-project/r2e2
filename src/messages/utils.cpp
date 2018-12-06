#include "utils.h"

#include <memory>
#include <stdexcept>

#include "lights/distant.h"
#include "samplers/halton.h"

namespace pbrt {

protobuf::Point2i to_protobuf(const Point2i& point) {
    protobuf::Point2i proto_point;
    proto_point.set_x(point.x);
    proto_point.set_y(point.y);
    return proto_point;
}

protobuf::Point2f to_protobuf(const Point2f& point) {
    protobuf::Point2f proto_point;
    proto_point.set_x(point.x);
    proto_point.set_y(point.y);
    return proto_point;
}

protobuf::Point3f to_protobuf(const Point3f& point) {
    protobuf::Point3f proto_point;
    proto_point.set_x(point.x);
    proto_point.set_y(point.y);
    proto_point.set_z(point.z);
    return proto_point;
}

protobuf::Vector3f to_protobuf(const Vector3f& vector) {
    protobuf::Vector3f proto_vector;
    proto_vector.set_x(vector.x);
    proto_vector.set_y(vector.y);
    proto_vector.set_z(vector.z);
    return proto_vector;
}

protobuf::Normal3f to_protobuf(const Normal3f& normal) {
    protobuf::Normal3f proto_normal;
    proto_normal.set_x(normal.x);
    proto_normal.set_y(normal.y);
    proto_normal.set_z(normal.z);
    return proto_normal;
}

protobuf::Bounds2i to_protobuf(const Bounds2i& bounds) {
    protobuf::Bounds2i proto_bounds;
    *proto_bounds.mutable_point_min() = to_protobuf(bounds.pMin);
    *proto_bounds.mutable_point_max() = to_protobuf(bounds.pMax);
    return proto_bounds;
}

protobuf::Bounds2f to_protobuf(const Bounds2f& bounds) {
    protobuf::Bounds2f proto_bounds;
    *proto_bounds.mutable_point_min() = to_protobuf(bounds.pMin);
    *proto_bounds.mutable_point_max() = to_protobuf(bounds.pMax);
    return proto_bounds;
}

protobuf::Bounds3f to_protobuf(const Bounds3f& bounds) {
    protobuf::Bounds3f proto_bounds;
    *proto_bounds.mutable_point_min() = to_protobuf(bounds.pMin);
    *proto_bounds.mutable_point_max() = to_protobuf(bounds.pMax);
    return proto_bounds;
}

protobuf::Matrix to_protobuf(const Matrix4x4& matrix) {
    protobuf::Matrix proto_matrix;
    for (size_t i = 0; i < 4; i++) {
        for (size_t j = 0; j < 4; j++) {
            proto_matrix.add_m(matrix.m[i][j]);
        }
    }
    return proto_matrix;
}

protobuf::RGBSpectrum to_protobuf(const RGBSpectrum& spectrum) {
    protobuf::RGBSpectrum proto_spectrum;
    proto_spectrum.add_c(spectrum[0]);
    proto_spectrum.add_c(spectrum[1]);
    proto_spectrum.add_c(spectrum[2]);
    return proto_spectrum;
}

protobuf::RayDifferential to_protobuf(const RayDifferential& ray) {
    protobuf::RayDifferential proto_ray;

    *proto_ray.mutable_o() = to_protobuf(ray.o);
    *proto_ray.mutable_d() = to_protobuf(ray.d);
    proto_ray.set_t_max(ray.tMax);
    proto_ray.set_time(ray.time);

    proto_ray.set_has_differentials(ray.hasDifferentials);
    if (ray.hasDifferentials) {
        *proto_ray.mutable_rx_origin() = to_protobuf(ray.rxOrigin);
        *proto_ray.mutable_ry_origin() = to_protobuf(ray.ryOrigin);
        *proto_ray.mutable_rx_direction() = to_protobuf(ray.rxDirection);
        *proto_ray.mutable_ry_direction() = to_protobuf(ray.ryDirection);
    }

    return proto_ray;
}

protobuf::AnimatedTransform to_protobuf(const AnimatedTransform& transform) {
    protobuf::AnimatedTransform proto_transform;
    *proto_transform.mutable_start_transform() =
        to_protobuf(transform.startTransform->GetMatrix());
    *proto_transform.mutable_end_transform() =
        to_protobuf(transform.endTransform->GetMatrix());
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

    if (tm.uv) {
        for (size_t i = 0; i < tm.nVertices; i++) {
            *proto_tm.add_uv() = to_protobuf(tm.uv[i]);
        }
    }

    if (tm.n) {
        for (size_t i = 0; i < tm.nVertices; i++) {
            *proto_tm.add_n() = to_protobuf(tm.n[i]);
        }
    }

    if (tm.s) {
        for (size_t i = 0; i < tm.nVertices; i++) {
            *proto_tm.add_s() = to_protobuf(tm.s[i]);
        }
    }

    return proto_tm;
}

protobuf::VisitNode to_protobuf(const RayState::TreeletNode& node) {
    protobuf::VisitNode proto_visit;
    proto_visit.set_treelet(node.treelet);
    proto_visit.set_node(node.node);
    if (node.transform) {
        *proto_visit.mutable_transform() =
            to_protobuf(node.transform->GetMatrix());
    }
    return proto_visit;
}

protobuf::RayState to_protobuf(const RayState& state) {
    protobuf::RayState proto_state;
    proto_state.set_sample_id(state.sample.id);
    proto_state.set_sample_num(state.sample.num);
    *proto_state.mutable_sample_pixel() = to_protobuf(state.sample.pixel);
    *proto_state.mutable_ray() = to_protobuf(state.ray);

    for (auto& tv : state.toVisit) {
        *proto_state.add_to_visit() = to_protobuf(tv);
    }

    if (state.hit.initialized()) {
        *proto_state.mutable_hit() = to_protobuf(*state.hit);
    }

    *proto_state.mutable_beta() = to_protobuf(state.beta);
    *proto_state.mutable_ld() = to_protobuf(state.Ld);
    proto_state.set_bounces(state.bounces);
    proto_state.set_remaining_bounces(state.remainingBounces);
    proto_state.set_is_shadow_ray(state.isShadowRay);

    return proto_state;
}

protobuf::Light to_protobuf(const std::shared_ptr<Light>& light) {
    protobuf::Light proto_light;

    switch (light->GetType()) {
    case LightType::Distant: {
        proto_light.set_type(protobuf::LIGHT_DISTANT);
        protobuf::DistantLight proto_distant;
        auto distant = std::dynamic_pointer_cast<const DistantLight>(light);
        *proto_distant.mutable_light_to_world() =
            to_protobuf(light->LightToWorld.GetMatrix());
        *proto_distant.mutable_l() = to_protobuf(distant->L);
        *proto_distant.mutable_dir() = to_protobuf(distant->wLight);
        proto_light.mutable_data()->PackFrom(proto_distant);
        break;
    }

    default:
        throw std::runtime_error("unsupported light type");
    }

    return proto_light;
}

protobuf::Sampler to_protobuf(const std::shared_ptr<Sampler>& sampler,
                              const Bounds2i& sampleBounds) {
    protobuf::Sampler proto_sampler;

    switch (sampler->GetType()) {
    case SamplerType::Halton: {
        proto_sampler.set_type(protobuf::SAMPLER_HALTON);
        protobuf::HaltonSampler proto_halton;
        auto halton = std::dynamic_pointer_cast<const HaltonSampler>(sampler);
        proto_halton.set_samples_per_pixel(halton->samplesPerPixel);
        proto_halton.set_sample_at_center(halton->sampleAtPixelCenter);
        *proto_halton.mutable_sample_bounds() = to_protobuf(sampleBounds);
        proto_sampler.mutable_data()->PackFrom(proto_halton);
        break;
    }

    default:
        throw std::runtime_error("unsupported sampler type");
    }

    return proto_sampler;
}

Point2i from_protobuf(const protobuf::Point2i& point) {
    return {point.x(), point.y()};
}

Point2f from_protobuf(const protobuf::Point2f& point) {
    return {point.x(), point.y()};
}

Point3f from_protobuf(const protobuf::Point3f& point) {
    return {point.x(), point.y(), point.z()};
}

Normal3f from_protobuf(const protobuf::Normal3f& normal) {
    return {normal.x(), normal.y(), normal.z()};
}

Vector3f from_protobuf(const protobuf::Vector3f& vector) {
    return {vector.x(), vector.y(), vector.z()};
}

Bounds2i from_protobuf(const protobuf::Bounds2i& bounds) {
    return {from_protobuf(bounds.point_min()),
            from_protobuf(bounds.point_max())};
}

Bounds2f from_protobuf(const protobuf::Bounds2f& bounds) {
    return {from_protobuf(bounds.point_min()),
            from_protobuf(bounds.point_max())};
}

Bounds3f from_protobuf(const protobuf::Bounds3f& bounds) {
    return {from_protobuf(bounds.point_min()),
            from_protobuf(bounds.point_max())};
}

Matrix4x4 from_protobuf(const protobuf::Matrix& proto_matrix) {
    Matrix4x4 matrix;
    for (size_t i = 0; i < 4; i++) {
        for (size_t j = 0; j < 4 and (4 * i + j < proto_matrix.m_size()); j++) {
            matrix.m[i][j] = proto_matrix.m(4 * i + j);
        }
    }
    return matrix;
}

RGBSpectrum from_protobuf(const protobuf::RGBSpectrum& proto_spectrum) {
    return RGBSpectrum::FromRGB(proto_spectrum.c().data());
}

RayDifferential from_protobuf(const protobuf::RayDifferential& proto_ray) {
    RayDifferential ray;

    ray.o = from_protobuf(proto_ray.o());
    ray.d = from_protobuf(proto_ray.d());
    ray.tMax = proto_ray.t_max();
    ray.time = proto_ray.time();
    ray.hasDifferentials = proto_ray.has_differentials();

    if (ray.hasDifferentials) {
        ray.rxOrigin = from_protobuf(proto_ray.rx_origin());
        ray.ryOrigin = from_protobuf(proto_ray.ry_origin());
        ray.rxDirection = from_protobuf(proto_ray.rx_direction());
        ray.ryDirection = from_protobuf(proto_ray.ry_direction());
    }

    return ray;
}

TriangleMesh from_protobuf(const protobuf::TriangleMesh& proto_tm) {
    Transform identity;
    std::vector<int> vertexIndices;
    std::vector<Point3f> p;
    std::vector<Vector3f> s;
    std::vector<Normal3f> n;
    std::vector<Point2f> uv;

    vertexIndices.reserve(proto_tm.n_triangles() * 3);
    p.reserve(proto_tm.n_vertices());

    for (size_t i = 0; i < proto_tm.vertex_indices_size(); i++) {
        vertexIndices.push_back(proto_tm.vertex_indices(i));
    }

    for (size_t i = 0; i < proto_tm.n_vertices(); i++) {
        p.push_back(from_protobuf(proto_tm.p(i)));
    }

    for (size_t i = 0; i < proto_tm.uv_size(); i++) {
        uv.push_back(from_protobuf(proto_tm.uv(i)));
    }

    for (size_t i = 0; i < proto_tm.s_size(); i++) {
        s.push_back(from_protobuf(proto_tm.s(i)));
    }

    for (size_t i = 0; i < proto_tm.n_size(); i++) {
        n.push_back(from_protobuf(proto_tm.n(i)));
    }

    return {identity,
            proto_tm.n_triangles(),
            vertexIndices.data(),
            proto_tm.n_vertices(),
            p.data(),
            s.data(),
            n.data(),
            uv.data(),
            nullptr,
            nullptr,
            nullptr};
}

RayState::TreeletNode from_protobuf(const protobuf::VisitNode& proto_node) {
    RayState::TreeletNode node;
    node.treelet = proto_node.treelet();
    node.node = proto_node.node();

    if (proto_node.has_transform()) {
        node.transform =
            std::make_shared<Transform>(from_protobuf(proto_node.transform()));
    }

    return node;
}

RayState from_protobuf(const protobuf::RayState& proto_state) {
    RayState state;

    state.sample.id = proto_state.sample_id();
    state.sample.num = proto_state.sample_num();
    state.sample.pixel = from_protobuf(proto_state.sample_pixel());
    state.ray = from_protobuf(proto_state.ray());

    for (size_t i = 0; i < proto_state.to_visit_size(); i++) {
        state.toVisit.push_back(from_protobuf(proto_state.to_visit(i)));
    }

    if (proto_state.has_hit()) {
        state.hit.reset(from_protobuf(proto_state.hit()));
    }

    state.beta = from_protobuf(proto_state.beta());
    state.Ld = from_protobuf(proto_state.ld());
    state.bounces = proto_state.bounces();
    state.remainingBounces = proto_state.remaining_bounces();
    state.isShadowRay = proto_state.is_shadow_ray();

    return state;
}

std::shared_ptr<Light> from_protobuf(const protobuf::Light& proto_light) {
    std::shared_ptr<Light> light;

    switch (proto_light.type()) {
    case protobuf::LIGHT_DISTANT: {
        protobuf::DistantLight proto_distant;
        proto_light.data().UnpackTo(&proto_distant);
        light = std::make_shared<DistantLight>(
            from_protobuf(proto_distant.light_to_world()),
            from_protobuf(proto_distant.l()),
            from_protobuf(proto_distant.dir()));
    }

    default:
        throw std::runtime_error("unsupported light type");
    }

    return light;
}

std::shared_ptr<Sampler> from_protobuf(const protobuf::Sampler& proto_sampler) {
    std::shared_ptr<Sampler> sampler;

    switch (proto_sampler.type()) {
    case protobuf::SAMPLER_HALTON: {
        protobuf::HaltonSampler proto_halton;
        proto_sampler.data().UnpackTo(&proto_halton);
        sampler = std::make_shared<HaltonSampler>(
            proto_halton.samples_per_pixel(),
            from_protobuf(proto_halton.sample_bounds()),
            proto_halton.sample_at_center());
        break;
    }

    default:
        throw std::runtime_error("unsupported sampler type");
    }

    return sampler;
}

}  // namespace pbrt
