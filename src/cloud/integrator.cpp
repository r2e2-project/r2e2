#include "integrator.h"

#include <cmath>
#include <cstdlib>
#include <deque>

using namespace std;

namespace pbrt {

vector<RayState> CloudIntegrator::Trace(const shared_ptr<CloudBVH> &treelet,
                                        RayState &&rayState) {
    vector<RayState> newRays;
    treelet->Trace(rayState);
    newRays.push_back(move(rayState));
    return newRays;
}

vector<RayState> CloudIntegrator::Shade(const shared_ptr<CloudBVH> &treelet,
                                        RayState &&rayState,
                                        vector<shared_ptr<Light>> &lights,
                                        MemoryArena &arena) {
    vector<RayState> newRays;
    SurfaceInteraction it;
    treelet->Intersect(rayState, &it);

    it.ComputeScatteringFunctions(rayState.ray, arena, true);
    if (!it.bsdf) {
        throw runtime_error("!it.bsdf");
    }

    const Distribution1D *distrib = lightDistribution->Lookup(it.p);
    auto &sampler = rayState.sampler;

    if (it.bsdf->NumComponents(BxDFType(BSDF_ALL & ~BSDF_SPECULAR)) > 0) {
        /* Let's pick a light at random */
        int nLights = (int)lights.size();
        int lightNum;
        Float lightSelectPdf;
        if (nLights > 0) {
            lightNum = std::min((int)(sampler.Get1D() * nLights), nLights - 1);
            lightSelectPdf = Float(1) / nLights;
        }

        const std::shared_ptr<Light> &light = lights[lightNum];
        Point2f uLight = sampler.Get2D();
        Point2f uScattering = sampler.Get2D();

        Vector3f wi;
        Float lightPdf;
        VisibilityTester visibility;
        Spectrum Li = light.Sample_Li(it, uLight, &wi, &lightPdf, &visibility);

        if (lightPdf > 0 && !Li.isBlack()) {
            Spectrum f;
            f = it.bsdf->f(it.wo, wi, bsdfFlags) * AbsDot(wi, it.shading.n);

            if (!f.isBlack()) {
                /* now we have to shoot the ray to the light source */
                RayState shadowRay{move(rayState)};
                shadowRay.ray = visibility.P0().SpawnRayTo(visibility.P1());
                shadowRay.lightSelectPdf = lightSelectPdf;
                shadowRay.lightPdf = lightPdf;
                shadowRay.f = f;
                shadowRay.Li = Li;
                shadowRay.isShadowRay = true;
                newRays.push_back(move(shadowRay));
            }
        }
    }

    return newRays;
}

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

        if (state.isDone) {
            filmTile->AddSample(state.sample.pFilm, state.L, state.weight);
        }
        else if (state.isShadowRay) {

        }

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
