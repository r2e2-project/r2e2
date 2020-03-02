#include "cloud/r2t2.h"

#include "cloud/raystate.h"
#include "core/camera.h"
#include "core/geometry.h"
#include "core/sampler.h"

using namespace std;

namespace pbrt::graphics {

RayStatePtr GenerateCameraRay(const shared_ptr<Camera> &camera,
                              const Point2i &pixel, const uint32_t sample,
                              const uint8_t maxDepth,
                              const Vector2i &sampleExtent,
                              shared_ptr<GlobalSampler> &sampler) {
    const auto samplesPerPixel = sampler->samplesPerPixel;
    const Float rayScale = 1 / sqrt((Float)samplesPerPixel);

    sampler->StartPixel(pixel);
    sampler->SetSampleNumber(sample);

    CameraSample cameraSample = sampler->GetCameraSample(pixel);

    RayStatePtr statePtr = RayState::Create();
    RayState &state = *statePtr;

    state.sample.id =
        (pixel.x + pixel.y * sampleExtent.x) * samplesPerPixel + sample;
    state.sample.dim = sampler->GetCurrentDimension();
    state.sample.pFilm = cameraSample.pFilm;
    state.sample.weight =
        camera->GenerateRayDifferential(cameraSample, &state.ray);
    state.ray.ScaleDifferentials(rayScale);
    state.remainingBounces = maxDepth - 1;
    state.StartTrace();

    return statePtr;
}

void AccumulateImage(const shared_ptr<Camera> &camera,
                     const vector<Sample> &rays) {
    const Bounds2i sampleBounds = camera->film->GetSampleBounds();
    unique_ptr<FilmTile> filmTile = camera->film->GetFilmTile(sampleBounds);

    unordered_set<uint64_t> sampleSet;

    for (const auto &ray : rays) {
        auto p = sampleSet.insert(ray.sampleId);
        filmTile->AddSample(ray.pFilm, ray.L, ray.weight, p.second);
    }

    camera->film->MergeFilmTile(move(filmTile));
}

}  // namespace pbrt::graphics
