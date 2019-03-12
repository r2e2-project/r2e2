#ifndef PBRT_CLOUD_RAYSTATE_H
#define PBRT_CLOUD_RAYSTATE_H

#include <deque>

#include "core/camera.h"
#include "core/geometry.h"
#include "core/interaction.h"
#include "core/sampler.h"
#include "core/transform.h"
#include "util/optional.h"

namespace pbrt {

struct RayState;
using RayStatePtr = std::unique_ptr<RayState>;

struct RayState {
    struct TreeletNode {
        uint32_t treelet{0};
        uint32_t node{0};
        uint8_t primitive{0};
        bool transformed{false};
    };

    struct Sample {
        size_t id;
        int64_t num;
        Point2i pixel;
        Point2f pFilm;
        Float weight;
    };

    RayState() = default;
    RayState(RayState &&) = default;

    /* disallow copying */
    RayState(const RayState &) = delete;
    RayState &operator=(const RayState &) = delete;

    bool trackRay{false};
    Sample sample;
    RayDifferential ray;
    Spectrum beta{1.f};
    Spectrum Ld{0.f};
    uint8_t remainingBounces{3};
    bool isShadowRay{false};

    bool hit{false};
    TreeletNode hitNode{};

    Transform hitTransform{};
    Transform rayTransform{};

    uint8_t toVisitHead{0};
    TreeletNode toVisit[64];

    bool toVisitEmpty() const { return toVisitHead == 0; }
    const TreeletNode &toVisitTop() const { return toVisit[toVisitHead - 1]; }
    void toVisitPush(TreeletNode &&t) { toVisit[toVisitHead++] = std::move(t); }
    void toVisitPop() { toVisitHead--; }

    void SetHit(const TreeletNode &node);
    void StartTrace();
    uint32_t CurrentTreelet() const;

    /* serialization */
    static std::string serialize(const RayStatePtr &, const bool = true);
    static RayStatePtr deserialize(const std::string &, const bool = true);
};

}  // namespace pbrt

#endif /* PBRT_CLOUD_RAYSTATE_H */
