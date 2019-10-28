#ifndef PBRT_CLOUD_RAYSTATE_H
#define PBRT_CLOUD_RAYSTATE_H

#include <memory>

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
    struct __attribute__((packed)) TreeletNode {
        uint32_t treelet{0};
        uint32_t node{0};
        uint8_t primitive{0};
        bool transformed{false};
    };

    struct Sample {
        uint64_t id;
        Point2f pFilm;
        Float weight;
        int dim;
    };

    RayState() = default;
    RayState(RayState &&) = default;

    /* disallow copying */
    RayState(const RayState &) = delete;
    RayState &operator=(const RayState &) = delete;

    bool trackRay{false};
    mutable uint16_t hop{0};

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

    /* RayState is serialized up until this point, and the serialized data
       will be stored below */

    size_t serializedSize{0};
    char serialized[1400];

    int64_t SampleNum(const uint32_t spp);
    Point2i SamplePixel(const Vector2i &extent, const uint32_t spp);

    bool toVisitEmpty() const { return toVisitHead == 0; }
    const TreeletNode &toVisitTop() const { return toVisit[toVisitHead - 1]; }
    void toVisitPush(TreeletNode &&t) { toVisit[toVisitHead++] = std::move(t); }
    void toVisitPop() { toVisitHead--; }

    void SetHit(const TreeletNode &node);
    void StartTrace();
    uint32_t CurrentTreelet() const;

    uint64_t PathID() const { return sample.id; }

    /* serialization */
    size_t Size() const;
    size_t SerializedSize() const { return serializedSize; }
    size_t Serialize(const bool compress = true);
    void Deserialize(const char *data, const size_t len,
                     const bool decompress = true);
};

}  // namespace pbrt

#endif /* PBRT_CLOUD_RAYSTATE_H */
