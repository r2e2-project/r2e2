#include <cstdlib>
#include <iostream>
#include <fstream>
#include "cloud/bvh.h"
#include "cloud/manager.h"
#include "cloud/r2t2.h"
#include "core/camera.h"
#include "core/geometry.h"
#include "core/transform.h"
#include "messages/utils.h"
#include "util/exception.h"
#include "cloud/raystate.h"

using namespace std;

namespace pbrt {
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
}

int main(int argc, char *argv[]) {
    using namespace pbrt;

    if (argc < 5) {
        cerr << argv[0] << " scene spp static0 pairwise" << endl;
        exit(EXIT_FAILURE);
    }

    PbrtOptions.nThreads = 1;

    global::manager.init(argv[1]);

    vector<unique_ptr<Transform>> transformCache;
    auto camera = loadCamera(transformCache);
    auto sampler = loadSampler();
    auto lights = loadLights();
    auto fakeScene = loadFakeScene();

    uint64_t numTreelets = global::manager.treeletCount();

    vector<unique_ptr<CloudBVH>> treelets;
    treelets.resize(numTreelets);

    /* let's load all the treelets */
    for (size_t i = 0; i < treelets.size(); i++) {
        treelets[i] = make_unique<CloudBVH>(i);
    }

    for (auto &light : lights) {
        light->Preprocess(fakeScene);
    }

    const auto sampleExtent = camera->film->GetSampleBounds().Diagonal();
    const Bounds2i sampleBounds = camera->film->GetSampleBounds();
    const size_t samplesPerPixel = stoul(argv[2]);
    const uint8_t maxDepth = 5;

    list<RayStatePtr> rayList;
    vector<Sample> samples;


    vector<uint64_t> treeletVisits(numTreelets);
    vector<vector<uint64_t>> treeletEdges(numTreelets, vector<uint64_t>(numTreelets));

    for (size_t sample = 0; sample < samplesPerPixel; sample++) {
        for (Point2i pixel : sampleBounds) {
            if (!InsideExclusive(pixel, sampleBounds)) continue;

            RayStatePtr statePtr = graphics::GenerateCameraRay(
                camera, pixel, sample, maxDepth, sampleExtent, sampler);

            rayList.emplace_back(move(statePtr));
        }
    }

    while (!rayList.empty()) {
        RayStatePtr theRayPtr = move(rayList.front());
        RayState &theRay = *theRayPtr;
        rayList.pop_front();

        const TreeletId rayTreeletId = theRay.CurrentTreelet();
        treeletVisits[rayTreeletId]++;

        if (!theRay.toVisitEmpty()) {
            auto newRayPtr = graphics::TraceRay(move(theRayPtr),
                                                *treelets[rayTreeletId]);
            auto &newRay = *newRayPtr;

            const bool hit = newRay.HasHit();
            const bool emptyVisit = newRay.toVisitEmpty();

            if (!emptyVisit) {
                CHECK_NE(newRay.CurrentTreelet(), rayTreeletId);
                treeletEdges[rayTreeletId][newRay.CurrentTreelet()]++;
            }

            if (newRay.IsShadowRay()) {
                if (hit || emptyVisit) {
                    newRay.Ld = hit ? 0.f : newRay.Ld;
                    samples.emplace_back(*newRayPtr);
                } else {
                    rayList.push_back(move(newRayPtr));
                }
            } else if (!emptyVisit || hit) {
                rayList.push_back(move(newRayPtr));
            } else if (emptyVisit) {
                newRay.Ld = 0.f;
                samples.emplace_back(*newRayPtr);
            }
        } else if (theRay.HasHit()) {
            MemoryArena arena;
            RayStatePtr bounceRay, shadowRay;
            tie(bounceRay, shadowRay) =
                graphics::ShadeRay(move(theRayPtr), *treelets[rayTreeletId],
                                   lights, sampleExtent, sampler, maxDepth, arena);

            if (bounceRay != nullptr) {
                rayList.push_back(move(bounceRay));
            }

            if (shadowRay != nullptr) {
                rayList.push_back(move(shadowRay));
            }
        }
    }

    ofstream static0(argv[3]);
    ofstream pairwise(argv[4]);

    for (uint32_t treeletID = 0; treeletID < numTreelets; treeletID++) {
        static0 << treeletID << " " << treeletVisits[treeletID] << endl;
        for (uint32_t altID = treeletID + 1; altID < numTreelets; altID++) {
            uint64_t count = treeletEdges[treeletID][altID];
            count += treeletEdges[altID][treeletID];
            if (count > 0) {
                pairwise << treeletID << " " << altID << " " << count << endl;
            }
        }
    }
}
