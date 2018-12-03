#ifndef PBRT_CLOUD_RAYSTATE_H
#define PBRT_CLOUD_RAYSTATE_H

#include <stack>

#include "core/camera.h"
#include "core/geometry.h"
#include "core/interaction.h"
#include "core/sampler.h"
#include "core/transform.h"
#include "util/optional.h"

namespace pbrt {

struct RayState {
    struct TreeletNode {
        uint32_t treelet{0};
        uint32_t node{0};
        std::shared_ptr<Transform> transform{nullptr};
    };

    RayState &operator=(const RayState &) = delete;
    RayState(const RayState &) = delete;
    RayState() = default;
    RayState(RayState &&) = default;

    std::unique_ptr<Sampler> sampler;
    size_t sampleIdx;
    RayDifferential ray;

    /* Traversing the BVH */
    std::stack<TreeletNode> toVisit;
    Optional<TreeletNode> hit;

    Spectrum beta{1.f};
    Spectrum Ld{0.f};

    uint8_t bounces{0};
    uint8_t remainingBounces{3};
    bool isShadowRay{false};

    void StartTrace() {
        hit.clear();
        // toVisit = {};
        toVisit.push({});
    }
};

}  // namespace pbrt

#endif /* PBRT_CLOUD_RAYSTATE_H */
