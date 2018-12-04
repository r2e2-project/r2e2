#ifndef PBRT_CLOUD_INTEGRATOR_H
#define PBRT_CLOUD_INTEGRATOR_H

#include "cloud/bvh.h"
#include "cloud/raystate.h"
#include "core/camera.h"
#include "core/integrator.h"
#include "core/lightdistrib.h"
#include "core/scene.h"

namespace pbrt {

class CloudIntegrator : public Integrator {
  public:
    CloudIntegrator(const int maxDepth, std::shared_ptr<const Camera> camera,
                    std::shared_ptr<Sampler> sampler,
                    const Bounds2i &pixelBounds)
        : maxDepth(maxDepth),
          camera(camera),
          sampler(sampler),
          pixelBounds(pixelBounds) {}

    void Preprocess(const Scene &scene, Sampler &sampler);
    void Render(const Scene &scene);

    static RayState Trace(RayState &&rayState,
                          const std::shared_ptr<CloudBVH> &treelet);

    static std::vector<RayState> Shade(
        RayState &&rayState, const std::shared_ptr<CloudBVH> &treelet,
        const std::vector<std::shared_ptr<Light>> &lights,
        std::shared_ptr<Sampler> &sampler, MemoryArena &arena);

  private:
    const int maxDepth;
    std::shared_ptr<const Camera> camera;
    std::shared_ptr<Sampler> sampler;
    std::shared_ptr<CloudBVH> bvh;
    const Bounds2i pixelBounds;

    MemoryArena arena;
};

CloudIntegrator *CreateCloudIntegrator(const ParamSet &params,
                                       std::shared_ptr<Sampler> sampler,
                                       std::shared_ptr<const Camera> camera);

}  // namespace pbrt

#endif /* PBRT_CLOUD_INTEGRATOR_H */
