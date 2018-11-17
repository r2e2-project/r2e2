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

    lightDistribution = CreateLightSampleDistribution("spatial", scene);
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
            state.sampler = sampler->Clone(0);
            state.sample = state.sampler->GetCameraSample(pixel);
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
    for (RayState &rayState : rayStates) {
        if (rayState.weight > 0)
            rayState.L = Li(rayState, scene, *rayState.sampler, arena);
        filmTile->AddSample(rayState.sample.pFilm, rayState.L, rayState.weight);
        arena.Reset();
    }

    /* Create the final output */
    camera->film->MergeFilmTile(std::move(filmTile));
    camera->film->WriteImage();
}

Spectrum CloudIntegrator::Li(RayState &rayState, const Scene &scene,
                             Sampler &sampler, MemoryArena &arena,
                             int depth) const {
    Spectrum beta{1.f};
    rayState.StartTrace();

    while (not rayState.toVisit.empty()) {
        bvh->Trace(rayState);
    }

    bool foundIntersection = rayState.isect.initialized();

    if (!foundIntersection) {
        for (const auto &light : scene.infiniteLights) {
            rayState.L += beta * light->Le(rayState.ray);
        }
    }

    if (!foundIntersection) return rayState.L;

    rayState.isect->ComputeScatteringFunctions(rayState.ray, arena, true);
    if (!rayState.isect->bsdf) {
        throw runtime_error("!isect.bsdf");
    }

    const Distribution1D *distrib =
        lightDistribution->Lookup(rayState.isect->p);

    if (rayState.isect->bsdf->NumComponents(
            BxDFType(BSDF_ALL & ~BSDF_SPECULAR)) > 0) {
        Spectrum Ld =
            beta * UniformSampleOneLight(*rayState.isect, scene, arena, sampler,
                                         false, distrib);
        CHECK_GE(Ld.y(), 0.f);
        rayState.L += Ld;
    }

    return rayState.L;

    /* static size_t n = 0;
    n++;
    const Float color[] = {(Float)((n + 200) % 509) / 509,
                           (Float)(n % 512) / 512,
                           (Float)((n + 400) % 287) / 287};
    return RGBSpectrum::FromRGB(color); */
}

CloudIntegrator *CreateCloudIntegrator(const ParamSet &params,
                                       shared_ptr<Sampler> sampler,
                                       shared_ptr<const Camera> camera) {
    Bounds2i pixelBounds = camera->film->GetSampleBounds();
    return new CloudIntegrator(camera, sampler, pixelBounds);
}

}  // namespace pbrt
