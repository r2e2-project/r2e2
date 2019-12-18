#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "cloud/manager.h"
#include "cloud/r2t2.h"
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
        vector<FinishedRay> finishedRays;

        global::manager.init(scenePath);

        vector<unique_ptr<Transform>> transformCache;
        shared_ptr<Camera> camera = loadCamera(scenePath, transformCache);

        size_t finishedRayCount = 0;
        for (string line; getline(cin, line);) {
            protobuf::RecordReader finishedReader{line};
            protobuf::FinishedRay proto;

            while (!finishedReader.eof()) {
                ++finishedRayCount;
                if (finishedReader.read(&proto)) {
                    finishedRays.push_back(from_protobuf(proto));
                }
            }
        }

        graphics::AccumulateImage(camera, finishedRays);

        /* Create the final output */
        camera->film->WriteImage();

        cerr << finishedRayCount << " finished ray(s)." << endl;
        cerr << "output written." << endl;
    } catch (const exception &e) {
        print_exception(argv[0], e);
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
