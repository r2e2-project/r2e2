#include <iostream>
#include <string>
#include <vector>

#include "cloud/bvh.h"
#include "cloud/manager.h"
#include "cloud/r2t2.h"
#include "cloud/raystate.h"
#include "messages/serialization.h"
#include "messages/utils.h"
#include "util/exception.h"

using namespace std;
using namespace pbrt;

void usage(const char *argv0) {
    cerr << argv0 << " SCENE-DATA RAYSTATES OUTPUT OUTPUT-FINISHED" << endl;
}

vector<shared_ptr<Light>> loadLights() {
    vector<shared_ptr<Light>> lights;
    auto reader = global::manager.GetReader(ObjectType::Lights);

    while (!reader->eof()) {
        protobuf::Light proto_light;
        reader->read(&proto_light);
        lights.push_back(move(light::from_protobuf(proto_light)));
    }

    return lights;
}

shared_ptr<Camera> loadCamera(vector<unique_ptr<Transform>> &transformCache) {
    auto reader = global::manager.GetReader(ObjectType::Camera);
    protobuf::Camera proto_camera;
    reader->read(&proto_camera);
    return camera::from_protobuf(proto_camera, transformCache);
}

shared_ptr<GlobalSampler> loadSampler() {
    auto reader = global::manager.GetReader(ObjectType::Sampler);
    protobuf::Sampler proto_sampler;
    reader->read(&proto_sampler);
    return sampler::from_protobuf(proto_sampler);
}

Scene loadFakeScene() {
    auto reader = global::manager.GetReader(ObjectType::Scene);
    protobuf::Scene proto_scene;
    reader->read(&proto_scene);
    return from_protobuf(proto_scene);
}

enum class Operation { Trace, Shade };

int main(int argc, char const *argv[]) {
    try {
        if (argc <= 0) {
            abort();
        }

        if (argc != 5) {
            usage(argv[0]);
            return EXIT_FAILURE;
        }

        /* CloudBVH requires this */
        PbrtOptions.nThreads = 1;

        const string scenePath{argv[1]};
        const string raysPath{argv[2]};
        const string outputPath{argv[3]};
        const string finishedPath{argv[4]};

        global::manager.init(scenePath);

        vector<RayStatePtr> rayStates;
        vector<RayStatePtr> outputRays;
        vector<RayStatePtr> finishedRays;

        /* loading all the rays */
        {
            protobuf::RecordReader reader{raysPath};
            while (!reader.eof()) {
                string rayStr;
                if (reader.read(&rayStr)) {
                    auto rayStatePtr = RayState::Create();
                    rayStatePtr->Deserialize(rayStr.data(), rayStr.length());
                    rayStates.push_back(move(rayStatePtr));
                }
            }
        }

        cerr << rayStates.size() << " RayState(s) loaded." << endl;

        if (!rayStates.size()) {
            return EXIT_SUCCESS;
        }

        MemoryArena arena;
        vector<unique_ptr<Transform>> transformCache;
        auto camera = loadCamera(transformCache);
        auto treelet = make_shared<CloudBVH>();
        auto sampler = loadSampler();
        auto lights = loadLights();
        auto fakeScene = loadFakeScene();

        const auto sampleExtent = camera->film->GetSampleBounds().Diagonal();

        for (auto &light : lights) {
            light->Preprocess(fakeScene);
        }

        for (auto &rayPtr : rayStates) {
            RayState &ray = *rayPtr;

            if (!ray.toVisitEmpty()) {
                const uint32_t rayTreelet = ray.toVisitTop().treelet;
                auto newRayPtr = graphics::TraceRay(move(rayPtr), *treelet);
                auto &newRay = *newRayPtr;

                const bool hit = newRay.hit;
                const bool emptyVisit = newRay.toVisitEmpty();

                if (newRay.isShadowRay) {
                    if (hit || emptyVisit) {
                        newRay.Ld = hit ? 0.f : newRay.Ld;
                        finishedRays.push_back(move(newRayPtr));
                    } else {
                        outputRays.push_back(move(newRayPtr));
                    }
                } else if (!emptyVisit || hit) {
                    outputRays.push_back(move(newRayPtr));
                } else if (emptyVisit) {
                    newRay.Ld = 0.f;
                    finishedRays.push_back(move(newRayPtr));
                }
            } else if (ray.hit) {
                RayStatePtr bounceRay, shadowRay;
                tie(bounceRay, shadowRay) =
                    graphics::ShadeRay(move(rayPtr), *treelet, lights,
                                       sampleExtent, sampler, arena);

                if (bounceRay != nullptr) {
                    outputRays.push_back(move(bounceRay));
                }

                if (shadowRay != nullptr) {
                    outputRays.push_back(move(shadowRay));
                }
            }
        }

        /* writing all the output rays */
        {
            protobuf::RecordWriter writer{outputPath};
            for (auto &rayState : outputRays) {
                const auto len = rayState->Serialize();
                writer.write(rayState->serialized.get() + 4, len - 4);
            }
        }

        /* writing all the finished rays */
        {
            protobuf::RecordWriter writer{finishedPath};
            for (auto &finished : finishedRays) {
                const auto len = finished->Serialize();
                writer.write(finished->serialized.get() + 4, len - 4);
            }
        }

        cerr << outputRays.size() << " output ray(s) and "
             << finishedRays.size() << " finished ray(s) were written." << endl;
    } catch (const exception &e) {
        print_exception(argv[0], e);
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
