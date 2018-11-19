#include "integrator.h"

#include <cmath>
#include <cstdlib>
#include <deque>

using namespace std;

namespace pbrt {

vector<RayState> CloudIntegrator::Trace(const shared_ptr<CloudBVH> &treelet,
                                        RayState &&rayState) {
    vector<RayState> result;
    treelet->Trace(rayState);
    result.push_back(move(rayState));
    return result;
}

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

    deque<RayState> rayQueue;

    /* Generate all the samples */
    for (Point2i pixel : sampleBounds) {
        sampler->StartPixel(pixel);

        if (!InsideExclusive(pixel, pixelBounds)) continue;

        RayState state;
        do {
            state.sampler = sampler->Clone(0);
            state.sample = state.sampler->GetCameraSample(pixel);
        } while (sampler->StartNextSample());

        rayQueue.push_back(move(state));
    }

    /* Generate all the rays */
    for (RayState &state : rayQueue) {
        state.weight =
            camera->GenerateRayDifferential(state.sample, &state.ray);
        state.ray.ScaleDifferentials(1 / sqrt((Float)sampler->samplesPerPixel));
        state.StartTrace();
    }

    while (not rayQueue.empty()) {
        RayState state = move(rayQueue.front());
        rayQueue.pop_front();

        if (not state.toVisit.empty()) {
            vector<RayState> newRays = Trace(bvh, move(state));
            for (auto &newRay : newRays) {
                rayQueue.push_back(move(newRay));
            }
        } else {
            bool foundIntersection = state.isect.initialized();

            if (foundIntersection) {
                state.isect->ComputeScatteringFunctions(state.ray, arena, true);
                if (!state.isect->bsdf) {
                    throw runtime_error("!isect.bsdf");
                }

                const Distribution1D *distrib =
                    lightDistribution->Lookup(state.isect->p);

                if (state.isect->bsdf->NumComponents(
                        BxDFType(BSDF_ALL & ~BSDF_SPECULAR)) > 0) {
                    Spectrum Ld =
                        UniformSampleOneLight(*state.isect, scene, arena,
                                              *state.sampler, false, distrib);
                    CHECK_GE(Ld.y(), 0.f);
                    state.L += Ld;
                }
            }

            filmTile->AddSample(state.sample.pFilm, state.L, state.weight);
            arena.Reset();
        }
    }

    /* Create the final output */
    camera->film->MergeFilmTile(std::move(filmTile));
    camera->film->WriteImage();
}

CloudIntegrator *CreateCloudIntegrator(const ParamSet &params,
                                       shared_ptr<Sampler> sampler,
                                       shared_ptr<const Camera> camera) {
    Bounds2i pixelBounds = camera->film->GetSampleBounds();
    return new CloudIntegrator(camera, sampler, pixelBounds);
}

}  // namespace pbrt
