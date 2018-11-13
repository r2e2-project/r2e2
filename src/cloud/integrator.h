#ifndef PBRT_CLOUD_INTEGRATOR_H
#define PBRT_CLOUD_INTEGRATOR_H

#include "core/camera.h"
#include "core/integrator.h"

namespace pbrt {

class CloudIntegrator : public Integrator {
  public:
    CloudIntegrator(std::shared_ptr<const Camera> camera,
                    std::shared_ptr<Sampler> sampler,
                    const Bounds2i &pixelBounds)
        : camera(camera), sampler(sampler), pixelBounds(pixelBounds) {}
    void Preprocess(const Scene &scene, Sampler &sampler) {}
    void Render(const Scene &scene);
    Spectrum Li(const RayDifferential &ray, const Scene &scene,
                Sampler &sampler, MemoryArena &arena, int depth = 0) const;

  protected:
    std::shared_ptr<const Camera> camera;

  private:
    std::shared_ptr<Sampler> sampler;
    const Bounds2i pixelBounds;
};

CloudIntegrator *CreateCloudIntegrator(const ParamSet &params,
                                       std::shared_ptr<Sampler> sampler,
                                       std::shared_ptr<const Camera> camera);

}  // namespace pbrt

#endif /* PBRT_CLOUD_INTEGRATOR_H */
