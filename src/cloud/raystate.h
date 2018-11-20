#ifndef PBRT_CLOUD_RAYSTATE_H
#define PBRT_CLOUD_RAYSTATE_H

#include <stack>

#include "core/camera.h"
#include "core/geometry.h"
#include "core/interaction.h"
#include "core/sampler.h"
#include "util/optional.h"

namespace pbrt {

struct RayState {
    std::unique_ptr<Sampler> sampler;
    CameraSample sample;
    RayDifferential ray;

    /* Traversing the BVH */
    std::stack<std::pair<uint32_t, uint32_t>> toVisit;
    Optional<std::pair<uint32_t, uint32_t>> hit;

    Float lightSelectPdf;
    Float lightPdf;
    Float f;
    Float Li;
    Float weight;
    Spectrum L{0.f};

    bool isShadowRay{false};

    void StartTrace() {
        hit.clear();
        toVisit.emplace(0, 0);
    }
};

}  // namespace pbrt

#endif /* PBRT_CLOUD_RAYSTATE_H */
