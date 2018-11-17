#include "integrator.h"

#include <cmath>
#include <cstdlib>
#include <stack>

using namespace std;

namespace pbrt {

void CloudIntegrator::Preprocess(const Scene &scene, Sampler &sampler) {
    bvh = dynamic_pointer_cast<CloudBVH>(scene.aggregate);
    if (bvh == nullptr) {
        throw runtime_error("Top-level primitive must be a CloudBVH");
    }
}

void CloudIntegrator::Render(const Scene &scene) {
    Preprocess(scene, *sampler);

    MemoryArena arena;

    Bounds2i sampleBounds = camera->film->GetSampleBounds();

    std::unique_ptr<FilmTile> filmTile =
        camera->film->GetFilmTile(sampleBounds);

    vector<RayState> rayStates;

    /* Generate all the samples */
    for (Point2i pixel : sampleBounds) {
        sampler->StartPixel(pixel);

        if (!InsideExclusive(pixel, pixelBounds)) continue;

        RayState state;
        do {
            state.sample = sampler->GetCameraSample(pixel);
        } while (sampler->StartNextSample());

        rayStates.push_back(move(state));
    }

    /* Generate all the rays */
    for (RayState &state : rayStates) {
        state.weight =
            camera->GenerateRayDifferential(state.sample, &state.ray);
        state.ray.ScaleDifferentials(1 / sqrt((Float)sampler->samplesPerPixel));
    }

    /* Find the radiance for all the rays */
    for (RayState &state : rayStates) {
        if (state.weight > 0) state.L = Li(state.ray, scene, *sampler, arena);
        filmTile->AddSample(state.sample.pFilm, state.L, state.weight);
        arena.Reset();
    }

    /* Create the final output */
    camera->film->MergeFilmTile(std::move(filmTile));
    camera->film->WriteImage();
}

Spectrum CloudIntegrator::Li(const RayDifferential &ray, const Scene &scene,
                             Sampler &sampler, MemoryArena &arena,
                             int depth) const {
    static size_t n = 0;
    n++;
    const Float color[] = {(Float)((n + 200) % 509) / 509,
                           (Float)(n % 512) / 512,
                           (Float)((n + 400) % 287) / 287};
    return RGBSpectrum::FromRGB(color);
}

CloudIntegrator *CreateCloudIntegrator(const ParamSet &params,
                                       shared_ptr<Sampler> sampler,
                                       shared_ptr<const Camera> camera) {
    Bounds2i pixelBounds = camera->film->GetSampleBounds();
    return new CloudIntegrator(camera, sampler, pixelBounds);
}

}  // namespace pbrt
