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

    Float weight;
    Spectrum beta{1.f};
    Spectrum Ld{0.f};

    uint8_t remainingBounces{3};
    bool isShadowRay{false};

    void StartTrace() {
        hit.clear();
        toVisit = {};
        toVisit.emplace(0, 0);
    }
};

}  // namespace pbrt

#endif /* PBRT_CLOUD_RAYSTATE_H */
