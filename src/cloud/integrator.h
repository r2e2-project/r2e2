#ifndef PBRT_CLOUD_INTEGRATOR_H
#define PBRT_CLOUD_INTEGRATOR_H

#include <memory>

#include "cloud/bvh.h"
#include "cloud/raystate.h"
#include "core/camera.h"
#include "core/integrator.h"
#include "core/lightdistrib.h"
#include "core/scene.h"

namespace pbrt {

class CloudIntegrator : public Integrator {
  public:
    struct SampleData {
        CameraSample sample;
        Spectrum L{0.f};
        Float weight{1.f};
    };

    CloudIntegrator(const int maxDepth, std::shared_ptr<const Camera> camera,
                    std::shared_ptr<GlobalSampler> sampler,
                    const Bounds2i &pixelBounds)
        : maxDepth(maxDepth),
          camera(camera),
          sampler(sampler),
          pixelBounds(pixelBounds) {}

    void Preprocess(const Scene &scene, Sampler &sampler);
    void Render(const Scene &scene);

    static RayStatePtr Trace(RayStatePtr &&rayState, const CloudBVH &treelet);

    static std::pair<RayStatePtr, RayStatePtr> Shade(
        RayStatePtr &&rayState, const CloudBVH &treelet,
        const std::vector<std::shared_ptr<Light>> &lights,
        const Vector2i &sampleExtent, std::shared_ptr<GlobalSampler> &sampler,
        MemoryArena &arena);

  private:
    const int maxDepth;
    std::shared_ptr<const Camera> camera;
    std::shared_ptr<GlobalSampler> sampler;
    std::shared_ptr<CloudBVH> bvh;
    const Bounds2i pixelBounds;

    MemoryArena arena;
};

CloudIntegrator *CreateCloudIntegrator(const ParamSet &params,
                                       std::shared_ptr<Sampler> sampler,
                                       std::shared_ptr<const Camera> camera);

}  // namespace pbrt

#endif /* PBRT_CLOUD_INTEGRATOR_H */
