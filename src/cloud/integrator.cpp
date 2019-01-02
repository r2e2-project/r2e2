#include "integrator.h"

#include <cmath>
#include <cstdlib>
#include <deque>
#include <iterator>

#include "cloud/stats.h"
#include "core/paramset.h"

using namespace std;

namespace pbrt {

STAT_COUNTER("Integrator/Camera rays traced", nCameraRays);
STAT_COUNTER("Intersections/Regular ray intersection tests",
             nIntersectionTests);
STAT_COUNTER("Intersections/Shadow ray intersection tests", nShadowTests);
STAT_INT_DISTRIBUTION("Integrator/Path length", pathLength);

RayState CloudIntegrator::Trace(RayState &&rayState,
                                const shared_ptr<CloudBVH> &treelet) {
    RayState result{move(rayState)};
    treelet->Trace(result);
    return result;
}

vector<RayState> CloudIntegrator::Shade(RayState &&rayState,
                                        const shared_ptr<CloudBVH> &treelet,
                                        const vector<shared_ptr<Light>> &lights,
                                        shared_ptr<Sampler> &sampler,
                                        MemoryArena &arena) {
    vector<RayState> newRays;

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
            RayState newRay;
            newRay.beta = rayState.beta * f * AbsDot(wi, it.shading.n) / pdf;
            newRay.ray = it.SpawnRay(wi);
            newRay.bounces = rayState.bounces + 1;
            newRay.remainingBounces = rayState.remainingBounces - 1;
            newRay.sample = rayState.sample;
            newRay.StartTrace();
            newRays.push_back(move(newRay));

            ++nIntersectionTests;
        }
    } else {
        /* we're done with this path */
        global::workerStats.finishedPaths++;
        ReportValue(pathLength, rayState.bounces);
    }

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

                ++nShadowTests;
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
    Bounds2i sampleBounds = camera->film->GetSampleBounds();
    unique_ptr<FilmTile> filmTile = camera->film->GetFilmTile(sampleBounds);

    deque<RayState> rayQueue;
    deque<RayState> finishedRays;
    vector<SampleData> cameraSamples;

    /* Generate all the samples */
    size_t i = 0;
    for (Point2i pixel : sampleBounds) {
        sampler->StartPixel(pixel);

        if (!InsideExclusive(pixel, pixelBounds)) continue;

        size_t sample_num = 0;
        do {
            SampleData sampleData;
            sampleData.sample = sampler->GetCameraSample(pixel);

            RayState state;
            state.sample.id = i++;
            state.sample.num = sample_num++;
            state.sample.pixel = pixel;
            sampleData.weight =
                camera->GenerateRayDifferential(sampleData.sample, &state.ray);
            state.ray.ScaleDifferentials(1 /
                                         sqrt((Float)sampler->samplesPerPixel));
            state.remainingBounces = maxDepth;
            state.StartTrace();

            rayQueue.push_back(move(state));
            cameraSamples.push_back(move(sampleData));

            ++nIntersectionTests;
            ++nCameraRays;
        } while (sampler->StartNextSample());
    }

    while (not rayQueue.empty()) {
        RayState state = move(rayQueue.back());
        rayQueue.pop_back();

        if (!state.toVisit.empty()) {
            auto newRay = Trace(move(state), bvh);
            const bool hit = newRay.hit.initialized();
            const bool emptyVisit = newRay.toVisit.empty();

            if (newRay.isShadowRay) {
                if (hit) {
                    continue; /* discard */
                } else if (emptyVisit) {
                    finishedRays.push_back(move(newRay));
                } else {
                    rayQueue.push_back(move(newRay));
                }
            } else if (!emptyVisit || hit) {
                rayQueue.push_back(move(newRay));
            }
        } else if (state.hit.initialized()) {
            auto newRays =
                Shade(move(state), bvh, scene.lights, sampler, arena);
            for (auto &newRay : newRays) {
                rayQueue.push_back(move(newRay));
            }
        } else {
            ReportValue(pathLength, state.bounces);
        }
    }

    for (const auto &state : finishedRays) {
        Spectrum L = state.beta * state.Ld;
        if (L.HasNaNs() || L.y() < -1e-5 || isinf(L.y())) L = Spectrum(0.f);
        cameraSamples[state.sample.id].L += L;
    }

    for (const auto &sampleData : cameraSamples) {
        filmTile->AddSample(sampleData.sample.pFilm, sampleData.L,
                            sampleData.weight);
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
