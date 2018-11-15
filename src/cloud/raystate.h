#ifndef PBRT_CLOUD_RAYSTATE_H
#define PBRT_CLOUD_RAYSTATE_H

#include <stack>

#include "core/camera.h"
#include "core/interaction.h"
#include "core/geometry.h"
#include "util/optional.h"

namespace pbrt {

struct RayState {
    CameraSample sample;
    RayDifferential ray;
    Optional<SurfaceInteraction> interaction;
    std::stack<std::pair<uint32_t, uint32_t>> toVisit;
    Float weight;
    Spectrum L;

    void StartTrace() {
        interaction.clear();
        toVisit.emplace(0, 0);
    }
};

}  // namespace pbrt

#endif /* PBRT_CLOUD_RAYSTATE_H */
