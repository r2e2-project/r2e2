#include <fstream>
#include <iostream>
#include <memory>
#include <sstream>
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

void usage(const char *argv0) { cerr << argv0 << " SCENE-DATA" << endl; }

shared_ptr<Camera> loadCamera(const string &scenePath,
                              vector<unique_ptr<Transform>> &transformCache) {
    auto reader = global::manager.GetReader(ObjectType::Camera);
    protobuf::Camera proto_camera;
    reader->read(&proto_camera);
    return camera::from_protobuf(proto_camera, transformCache);
}

int main(int argc, char const *argv[]) {
    try {
        if (argc <= 0) {
            abort();
        }

        if (argc != 2) {
            usage(argv[0]);
            return EXIT_FAILURE;
        }

        const string scenePath{argv[1]};

        global::manager.init(scenePath);

        vector<unique_ptr<Transform>> transformCache;
        shared_ptr<Camera> camera = loadCamera(scenePath, transformCache);
        const Bounds2i sampleBounds = camera->film->GetSampleBounds();
        unique_ptr<FilmTile> filmTile = camera->film->GetFilmTile(sampleBounds);

        Sample sample;

        for (string line; getline(cin, line);) {
            cerr << "Processing " << line << "... ";
            ifstream fin{line};
            ostringstream buffer;
            buffer << fin.rdbuf();
            const string dataStr = buffer.str();
            const char *data = dataStr.data();

            for (size_t offset = 0; offset < dataStr.size();) {
                const auto len =
                    *reinterpret_cast<const uint32_t *>(data + offset);
                offset += 4;

                sample.Deserialize(data + offset, len);
                filmTile->AddSample(sample.pFilm, sample.L, sample.weight);
                offset += len;
            }

            cerr << "done." << endl;
        }

        /* Create the final output */
        camera->film->MergeFilmTile(move(filmTile));
        camera->film->WriteImage();
    } catch (const exception &e) {
        print_exception(argv[0], e);
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
