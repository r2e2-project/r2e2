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

template <class T, uint8_t N>
class SimpleStack {
  public:
    bool empty() const { return head == 0; }
    const T &top() const { return items[head - 1]; }
    void push(T &&t) { items[head++] = std::move(t); }
    void pop() { head--; }
    uint8_t size() const { return head; }

  private:
    uint8_t head{0};
    T items[N];
};

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
    SimpleStack<TreeletNode, 64> toVisit{};

    void SetHit(const TreeletNode &node);
    void StartTrace();
    uint32_t CurrentTreelet() const;
};

}  // namespace pbrt

#endif /* PBRT_CLOUD_RAYSTATE_H */
