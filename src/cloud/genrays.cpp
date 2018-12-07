#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "core/camera.h"
#include "core/geometry.h"
#include "core/transform.h"
#include "messages/utils.h"
#include "util/exception.h"

using namespace std;
using namespace pbrt;

void usage(const char *argv0) { cerr << argv0 << " SCENE-DATA OUTPUT" << endl; }

shared_ptr<Camera> loadCamera(const string &scenePath,
                              vector<unique_ptr<Transform>> &transformCache) {
    protobuf::RecordReader reader{scenePath + "/CAMERA"};
    protobuf::Camera proto_camera;
    reader.read(&proto_camera);
    return camera::from_protobuf(proto_camera, transformCache);
}

shared_ptr<Sampler> loadSampler(const string &scenePath) {
    protobuf::RecordReader reader{scenePath + "/SAMPLER"};
    protobuf::Sampler proto_sampler;
    reader.read(&proto_sampler);
    return sampler::from_protobuf(proto_sampler);
}

enum class Operation { Trace, Shade };

int main(int argc, char const *argv[]) {
    try {
        if (argc <= 0) {
            abort();
        }

        if (argc != 3) {
            usage(argv[0]);
            return EXIT_FAILURE;
        }

        const string scenePath{argv[1]};
        const string outputPath{argv[2]};

        vector<unique_ptr<Transform>> transformCache;
        shared_ptr<Sampler> sampler = loadSampler(scenePath);
        shared_ptr<Camera> camera = loadCamera(scenePath, transformCache);

        const Bounds2i sampleBounds = camera->film->GetSampleBounds();
        const uint8_t maxDepth = 5;
        const float rayScale = 1 / sqrt((Float)sampler->samplesPerPixel);

        protobuf::RecordWriter rayWriter{outputPath};

        /* Generate all the samples */
        size_t i = 0;
        for (Point2i pixel : sampleBounds) {
            sampler->StartPixel(pixel);

            if (!InsideExclusive(pixel, sampleBounds)) continue;

            size_t sample_num = 0;
            do {
                CameraSample cameraSample = sampler->GetCameraSample(pixel);
                RayState state;

                state.sample.id = i++;
                state.sample.num = sample_num++;
                state.sample.pixel = pixel;
                state.remainingBounces = maxDepth;
                camera->GenerateRayDifferential(cameraSample, &state.ray);
                state.ray.ScaleDifferentials(rayScale);
                state.StartTrace();

                rayWriter.write(to_protobuf(state));
            } while (sampler->StartNextSample());
        }
    } catch (const exception &e) {
        print_exception(argv[0], e);
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
