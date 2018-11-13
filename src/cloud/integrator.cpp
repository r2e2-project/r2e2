#include "integrator.h"

#include <cstdlib>

using namespace std;

namespace pbrt {

void CloudIntegrator::Render(const Scene &scene) {
    Preprocess(scene, *sampler);

    MemoryArena arena;

    Bounds2i sampleBounds = camera->film->GetSampleBounds();
    Vector2i sampleExtent = sampleBounds.Diagonal();

    std::unique_ptr<FilmTile> filmTile =
        camera->film->GetFilmTile(sampleBounds);

    vector<CameraSample> samples;

    for (Point2i pixel : sampleBounds) {
        sampler->StartPixel(pixel);

        if (!InsideExclusive(pixel, pixelBounds)) continue;

        do {
            samples.push_back(sampler->GetCameraSample(pixel));
        } while (sampler->StartNextSample());
    }

    for (const CameraSample sample : samples) {
        RayDifferential ray;
        Float rayWeight = camera->GenerateRayDifferential(sample, &ray);
        ray.ScaleDifferentials(1 / std::sqrt((Float)sampler->samplesPerPixel));

        Spectrum L(0.f);
        if (rayWeight > 0) L = Li(ray, scene, *sampler, arena);

        filmTile->AddSample(sample.pFilm, L, rayWeight);
        arena.Reset();
    }

    camera->film->MergeFilmTile(std::move(filmTile));
    camera->film->WriteImage();
}

Spectrum CloudIntegrator::Li(const RayDifferential &ray, const Scene &scene,
                             Sampler &sampler, MemoryArena &arena,
                             int depth) const {
    static size_t n = 0;
    n++;
    const Float color[] = {(Float)((n + 200) % 600) / 600,
                           (Float)(n % 512) / 512,
                           (Float)((n + 400) % 300) / 300};
    return RGBSpectrum::FromRGB(color);
}

CloudIntegrator *CreateCloudIntegrator(const ParamSet &params,
                                       shared_ptr<Sampler> sampler,
                                       shared_ptr<const Camera> camera) {
    Bounds2i pixelBounds = camera->film->GetSampleBounds();
    return new CloudIntegrator(camera, sampler, pixelBounds);
}

}  // namespace pbrt
