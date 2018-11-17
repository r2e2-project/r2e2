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
    Optional<SurfaceInteraction> isect;
    std::stack<std::pair<uint32_t, uint32_t>> toVisit;
    Float weight;
    Spectrum L{0.f};

    void StartTrace() {
        isect.clear();
        toVisit.emplace(0, 0);
    }
};

}  // namespace pbrt

#endif /* PBRT_CLOUD_RAYSTATE_H */
