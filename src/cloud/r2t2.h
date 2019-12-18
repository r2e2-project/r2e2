#ifndef PBRT_CLOUD_R2T2_H
#define PBRT_CLOUD_R2T2_H

#include <memory>

#include "cloud/integrator.h"

namespace pbrt {

namespace graphics {

/* [defined in cloud/integrator.h]
   RayStatePtr Trace(RayStatePtr &&rayState,
                     const CloudBVH &treelet); */
constexpr auto TraceRay = &pbrt::CloudIntegrator::Trace;

/* [defined in cloud/integrator.h]
   pair<RayStatePtr, RayStatePtr> Shade(RayStatePtr &&rayState,
                                        const CloudBVH &treelet,
                                        const vector<shared_ptr<Light>> &lights,
                                        const Vector2i &sampleExtent,
                                        shared_ptr<GlobalSampler> &sampler,
                                        MemoryArena &arena); */
constexpr auto ShadeRay = &pbrt::CloudIntegrator::Shade;

RayStatePtr GenerateCameraRay(const std::shared_ptr<Camera> &camera,
                              const Point2i &pixel, const uint32_t sample_num,
                              const uint8_t maxDepth,
                              const Vector2i &sampleExtent,
                              std::shared_ptr<GlobalSampler> &sampler);

void AccumulateImage(const std::shared_ptr<Camera> &camera,
                     const std::vector<FinishedRay> &rays);

}  // namespace graphics

}  // namespace pbrt

#endif /* PBRT_CLOUD_GRAPHICS_H */
