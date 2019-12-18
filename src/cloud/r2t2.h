#ifndef PBRT_CLOUD_R2T2_H
#define PBRT_CLOUD_R2T2_H

#include "cloud/integrator.h"

namespace r2t2 {

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

}  // namespace graphics

}  // namespace r2t2

#endif /* PBRT_CLOUD_GRAPHICS_H */
