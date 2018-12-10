#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "cloud/manager.h"
#include "core/camera.h"
#include "core/geometry.h"
#include "core/transform.h"
#include "messages/utils.h"
#include "util/exception.h"

using namespace std;
using namespace pbrt;

void usage(const char *argv0) {
    cerr << argv0 << " SCENE-DATA SAMPLES-INDEX FINISHED..." << endl;
}

shared_ptr<Camera> loadCamera(const string &scenePath,
                              vector<unique_ptr<Transform>> &transformCache) {
    auto reader = global::manager.GetReader(SceneManager::Type::Camera);
    protobuf::Camera proto_camera;
    reader->read(&proto_camera);
    return camera::from_protobuf(proto_camera, transformCache);
}

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
        const string samplesPath{argv[2]};

        global::manager.init(scenePath);

        vector<unique_ptr<Transform>> transformCache;
        shared_ptr<Camera> camera = loadCamera(scenePath, transformCache);

        /* load all the camera samples */
        vector<CloudIntegrator::SampleData> cameraSamples;
        protobuf::RecordReader samplesReader{samplesPath};
        while (!samplesReader.eof()) {
            protobuf::SampleData proto_sample;
            samplesReader.read(&proto_sample);
            cameraSamples.push_back(from_protobuf(proto_sample));
        }

        cerr << cameraSamples.size() << " sample(s) loaded." << endl;

        for (string line; getline(cin, line);) {
            protobuf::RecordReader finishedReader{line};

            while(!finishedReader.eof()) {
                protobuf::RayState proto_ray;
                finishedReader.read(&proto_ray);
                auto rayState = from_protobuf(proto_ray);

                Spectrum L{0.f};
                L = rayState.Ld * rayState.beta;

                if (L.HasNaNs() || L.y() < -1e-5 || isinf(L.y())) {
                    L = Spectrum(0.f);
                }

                cameraSamples[rayState.sample.id].L += L;
            }
        }

        Bounds2i sampleBounds = camera->film->GetSampleBounds();
        unique_ptr<FilmTile> filmTile = camera->film->GetFilmTile(sampleBounds);

        for (const auto &sampleData : cameraSamples) {
            filmTile->AddSample(sampleData.sample.pFilm, sampleData.L,
                                sampleData.weight);
        }

        /* Create the final output */
        camera->film->MergeFilmTile(move(filmTile));
        camera->film->WriteImage();

        cout << "output written." << endl;
    } catch (const exception &e) {
        print_exception(argv[0], e);
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
