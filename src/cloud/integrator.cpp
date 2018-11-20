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
                                        const vector<shared_ptr<Light>> &lights,
                                        MemoryArena &arena) {
    vector<RayState> newRays;

    SurfaceInteraction it;
    rayState.ray.tMax = Infinity;
    treelet->Intersect(rayState, &it);

    it.ComputeScatteringFunctions(rayState.ray, arena, true);
    if (!it.bsdf) {
        throw runtime_error("!it.bsdf");
    }

    auto &sampler = rayState.sampler;
    const auto bsdfFlags = BxDFType(BSDF_ALL & ~BSDF_SPECULAR);

    if (it.bsdf->NumComponents(bsdfFlags) > 0) {
        /* Let's pick a light at random */
        int nLights = (int)lights.size();
        int lightNum;
        Float lightSelectPdf;
        if (nLights == 0) {
            return newRays;
        }

        lightSelectPdf = Float(1) / nLights;
        lightNum = min((int)(sampler->Get1D() * nLights), nLights - 1);
        const shared_ptr<Light> &light = lights[lightNum];

        Point2f uLight = sampler->Get2D();
        Vector3f wi;
        Float lightPdf;
        VisibilityTester visibility;
        Spectrum Li = light->Sample_Li(it, uLight, &wi, &lightPdf, &visibility);

        if (lightPdf > 0 && !Li.IsBlack()) {
            Spectrum f;
            f = it.bsdf->f(it.wo, wi, bsdfFlags) * AbsDot(wi, it.shading.n);

            if (!f.IsBlack()) {
                /* now we have to shoot the ray to the light source */
                RayState shadowRay{move(rayState)};
                shadowRay.ray = visibility.P0().SpawnRayTo(visibility.P1());
                shadowRay.Ld = (f * Li / lightPdf) / lightSelectPdf;
                shadowRay.isShadowRay = true;
                shadowRay.StartTrace();
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

    unique_ptr<FilmTile> filmTile = camera->film->GetFilmTile(sampleBounds);

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
        vector<RayState> newRays;

        if (not state.toVisit.empty()) {
            newRays = Trace(bvh, move(state));
        } else if (state.isDone) {
            if (state.L.HasNaNs() || state.L.y() < -1e-5 ||
                isinf(state.L.y())) {
                state.L = Spectrum(0.f);
            }

            filmTile->AddSample(state.sample.pFilm, state.L, state.weight);
            arena.Reset();
        } else if (state.isShadowRay) {
            if (!state.hit.initialized()) {
                state.L += state.Ld;
            }

            state.isDone = true;
            newRays.push_back(move(state));
        } else if (state.hit.initialized()) {
            newRays = Shade(bvh, move(state), scene.lights, arena);
        }

        for (auto &newRay : newRays) {
            rayQueue.push_back(move(newRay));
        }
    }

    /* Create the final output */
    camera->film->MergeFilmTile(move(filmTile));
    camera->film->WriteImage();
}

CloudIntegrator *CreateCloudIntegrator(const ParamSet &params,
                                       shared_ptr<Sampler> sampler,
                                       shared_ptr<const Camera> camera) {
    Bounds2i pixelBounds = camera->film->GetSampleBounds();
    return new CloudIntegrator(camera, sampler, pixelBounds);
}

}  // namespace pbrt
