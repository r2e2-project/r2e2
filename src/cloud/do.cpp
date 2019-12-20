#include <iostream>
#include <queue>
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
    cerr << argv0 << " SCENE-DATA CAMERA-RAYS" << endl;
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

        if (argc != 3) {
            usage(argv[0]);
            return EXIT_FAILURE;
        }

        /* CloudBVH requires this */
        PbrtOptions.nThreads = 1;

        const string scenePath{argv[1]};
        const string raysPath{argv[2]};

        global::manager.init(scenePath);

        queue<RayStatePtr> rayList;
        vector<Sample> samples;

        /* loading all the rays */
        {
            protobuf::RecordReader reader{raysPath};
            while (!reader.eof()) {
                string rayStr;

                if (reader.read(&rayStr)) {
                    auto rayStatePtr = RayState::Create();
                    rayStatePtr->Deserialize(rayStr.data(), rayStr.length());
                    rayList.push(move(rayStatePtr));
                }
            }
        }

        cerr << rayList.size() << " RayState(s) loaded." << endl;

        if (!rayList.size()) {
            return EXIT_SUCCESS;
        }

        /* prepare the scene */
        MemoryArena arena;
        vector<unique_ptr<Transform>> transformCache;
        auto camera = loadCamera(transformCache);
        auto sampler = loadSampler();
        auto lights = loadLights();
        auto fakeScene = loadFakeScene();

        vector<unique_ptr<CloudBVH>> treelets;
        treelets.resize(global::manager.treeletCount());

        /* let's load all the treelets */
        for (size_t i = 0; i < treelets.size(); i++) {
            treelets[i] = make_unique<CloudBVH>(i);
        }

        for (auto &light : lights) {
            light->Preprocess(fakeScene);
        }

        const auto sampleExtent = camera->film->GetSampleBounds().Diagonal();

        while (!rayList.empty()) {
            RayStatePtr theRayPtr = move(rayList.front());
            RayState &theRay = *theRayPtr;
            rayList.pop();

            const TreeletId rayTreeletId = theRay.CurrentTreelet();

            if (!theRay.toVisitEmpty()) {
                auto newRayPtr = graphics::TraceRay(move(theRayPtr),
                                                    *treelets[rayTreeletId]);
                auto &newRay = *newRayPtr;

                const bool hit = newRay.HasHit();
                const bool emptyVisit = newRay.toVisitEmpty();

                if (newRay.IsShadowRay()) {
                    if (hit || emptyVisit) {
                        newRay.Ld = hit ? 0.f : newRay.Ld;
                        samples.emplace_back(*newRayPtr);
                    } else {
                        rayList.push(move(newRayPtr));
                    }
                } else if (!emptyVisit || hit) {
                    rayList.push(move(newRayPtr));
                } else if (emptyVisit) {
                    newRay.Ld = 0.f;
                    samples.emplace_back(*newRayPtr);
                }
            } else if (theRay.HasHit()) {
                RayStatePtr bounceRay, shadowRay;
                tie(bounceRay, shadowRay) =
                    graphics::ShadeRay(move(theRayPtr), *treelets[rayTreeletId],
                                       lights, sampleExtent, sampler, arena);

                if (bounceRay != nullptr) {
                    rayList.push(move(bounceRay));
                }

                if (shadowRay != nullptr) {
                    rayList.push(move(shadowRay));
                }
            }
        }

        graphics::AccumulateImage(camera, samples);
        camera->film->WriteImage();
    } catch (const exception &e) {
        print_exception(argv[0], e);
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
