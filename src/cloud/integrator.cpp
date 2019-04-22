#include "integrator.h"

#include <cmath>
#include <cstdlib>
#include <deque>
#include <iterator>

#include "cloud/manager.h"
#include "cloud/stats.h"
#include "core/paramset.h"

using namespace std;

namespace pbrt {

STAT_COUNTER("Integrator/Camera rays traced", nCameraRays);
STAT_COUNTER("Intersections/Regular ray intersection tests",
             nIntersectionTests);
STAT_COUNTER("Intersections/Shadow ray intersection tests", nShadowTests);
STAT_INT_DISTRIBUTION("Integrator/Path length", pathLength);

RayStatePtr CloudIntegrator::Trace(RayStatePtr &&rayState,
                                   const shared_ptr<CloudBVH> &treelet) {
    treelet->Trace(*rayState);
    return move(rayState);
}

pair<vector<RayStatePtr>, bool> CloudIntegrator::Shade(
    RayStatePtr &&rayStatePtr, const shared_ptr<CloudBVH> &treelet,
    const vector<shared_ptr<Light>> &lights, shared_ptr<Sampler> &sampler,
    MemoryArena &arena) {
    vector<RayStatePtr> newRays;
    auto &rayState = *rayStatePtr;

    bool pathFinished = false;

    SurfaceInteraction it;
    rayState.ray.tMax = Infinity;
    treelet->Intersect(rayState, &it);

    it.ComputeScatteringFunctions(rayState.ray, arena, true);
    if (!it.bsdf) {
        throw runtime_error("!it.bsdf");
    }

    /* setting the sampler */
    sampler->StartPixel(rayState.sample.pixel);
    sampler->SetSampleNumber(rayState.sample.num);

    const auto bsdfFlags = BxDFType(BSDF_ALL & ~BSDF_SPECULAR);

    if (rayState.remainingBounces) {
        Vector3f wo = -rayState.ray.d, wi;
        Float pdf;
        BxDFType flags;
        Spectrum f = it.bsdf->Sample_f(wo, &wi, sampler->Get2D(), &pdf,
                                       BSDF_ALL, &flags);

        if (!f.IsBlack() && pdf > 0.f) {
            RayStatePtr newRayPtr = make_unique<RayState>();
            auto &newRay = *newRayPtr;

            newRay.trackRay = rayState.trackRay;
            newRay.beta = rayState.beta * f * AbsDot(wi, it.shading.n) / pdf;
            newRay.ray = it.SpawnRay(wi);
            newRay.remainingBounces = rayState.remainingBounces - 1;
            newRay.sample = rayState.sample;
            newRay.StartTrace();

            newRays.push_back(move(newRayPtr));

            ++nIntersectionTests;
        } else {
            pathFinished = true;
        }
    } else {
        /* we're done with this path */
        pathFinished = true;
    }

    if (it.bsdf->NumComponents(bsdfFlags) > 0) {
        /* Let's pick a light at random */
        int nLights = (int)lights.size();
        int lightNum;
        Float lightSelectPdf;
        if (nLights == 0) {
            return {move(newRays), pathFinished};
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
                RayStatePtr shadowRayPtr = move(rayStatePtr);
                auto &shadowRay = *shadowRayPtr;

                shadowRay.ray = visibility.P0().SpawnRayTo(visibility.P1());
                shadowRay.Ld = (f * Li / lightPdf) / lightSelectPdf;
                shadowRay.isShadowRay = true;
                shadowRay.StartTrace();

                newRays.push_back(move(shadowRayPtr));

                ++nShadowTests;
            }
        }
    }

    return {move(newRays), pathFinished};
}

void CloudIntegrator::Preprocess(const Scene &scene, Sampler &sampler) {
    bvh = dynamic_pointer_cast<CloudBVH>(scene.aggregate);
    if (bvh == nullptr) {
        throw runtime_error("Top-level primitive must be a CloudBVH");
    }
}

void CloudIntegrator::Render(const Scene &scene) {
    Preprocess(scene, *sampler);
    Bounds2i sampleBounds = camera->film->GetSampleBounds();
    unique_ptr<FilmTile> filmTile = camera->film->GetFilmTile(sampleBounds);

    deque<RayStatePtr> rayQueue;
    deque<RayStatePtr> finishedRays;

    /* Generate all the samples */
    size_t i = 0;
    for (Point2i pixel : sampleBounds) {
        sampler->StartPixel(pixel);

        if (!InsideExclusive(pixel, pixelBounds)) continue;

        size_t sample_num = 0;
        do {
            CameraSample cameraSample = sampler->GetCameraSample(pixel);

            RayStatePtr statePtr = make_unique<RayState>();
            auto &state = *statePtr;

            state.sample.id = i++;
            state.sample.num = sample_num++;
            state.sample.pixel = pixel;
            state.sample.pFilm = cameraSample.pFilm;
            state.sample.weight =
                camera->GenerateRayDifferential(cameraSample, &state.ray);
            state.ray.ScaleDifferentials(1 /
                                         sqrt((Float)sampler->samplesPerPixel));
            state.remainingBounces = maxDepth;
            state.StartTrace();

            rayQueue.push_back(move(statePtr));

            ++nIntersectionTests;
            ++nCameraRays;
        } while (sampler->StartNextSample());
    }

    while (not rayQueue.empty()) {
        RayStatePtr statePtr = move(rayQueue.back());
        RayState &state = *statePtr;
        rayQueue.pop_back();

        if (!state.toVisitEmpty()) {
            auto newRayPtr = Trace(move(statePtr), bvh);
            auto &newRay = *newRayPtr;
            const bool hit = newRay.hit;
            const bool emptyVisit = newRay.toVisitEmpty();

            if (newRay.isShadowRay) {
                if (hit) {
                    newRay.Ld = 0.f;
                    finishedRays.push_back(move(newRayPtr));
                    continue; /* discard */
                } else if (emptyVisit) {
                    finishedRays.push_back(move(newRayPtr));
                } else {
                    rayQueue.push_back(move(newRayPtr));
                }
            } else if (!emptyVisit || hit) {
                rayQueue.push_back(move(newRayPtr));
            } else {
                newRay.Ld = 0.f;
                finishedRays.push_back(move(newRayPtr));
            }
        } else if (state.hit) {
            auto newRays =
                Shade(move(statePtr), bvh, scene.lights, sampler, arena).first;
            for (auto &newRay : newRays) {
                rayQueue.push_back(move(newRay));
            }
        } else {
            throw runtime_error("unexpected ray state");
        }
    }

    struct CSample {
        Point2f pFilm;
        Spectrum L{0.f};
        Float weight{0.f};
    };

    unordered_map<size_t, CSample> allSamples;

    for (const auto &statePtr : finishedRays) {
        const auto &state = *statePtr;

        if (allSamples.count(state.sample.id) == 0) {
            allSamples[state.sample.id].pFilm = state.sample.pFilm;
            allSamples[state.sample.id].weight = state.sample.weight;
            allSamples[state.sample.id].L = 0.f;
        }

        Spectrum L = state.beta * state.Ld;
        if (L.HasNaNs() || L.y() < -1e-5 || isinf(L.y())) L = Spectrum(0.f);
        allSamples[state.sample.id].L += L;
    }

    cout << allSamples.size() << endl;

    for (const auto &kv : allSamples) {
        filmTile->AddSample(kv.second.pFilm, kv.second.L, kv.second.weight);
    }

    /* Create the final output */
    camera->film->MergeFilmTile(move(filmTile));
    camera->film->WriteImage();
}

CloudIntegrator *CreateCloudIntegrator(const ParamSet &params,
                                       shared_ptr<Sampler> sampler,
                                       shared_ptr<const Camera> camera) {
    const int maxDepth = params.FindOneInt("maxdepth", 5);
    Bounds2i pixelBounds = camera->film->GetSampleBounds();
    return new CloudIntegrator(maxDepth, camera, sampler, pixelBounds);
}

}  // namespace pbrt
