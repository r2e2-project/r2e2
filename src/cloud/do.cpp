#include <iostream>
#include <string>

#include "cloud/bvh.h"
#include "cloud/integrator.h"
#include "cloud/manager.h"
#include "cloud/raystate.h"
#include "messages/serialization.h"
#include "messages/utils.h"
#include "util/exception.h"

using namespace std;
using namespace pbrt;

void usage(const char *argv0) {
    cerr << argv0 << " RAYSTATES SCENE-DATA OUTPUT OUTPUT-FINISHED" << endl;
}

vector<shared_ptr<Light>> loadLights(const string &scenePath) {
    vector<shared_ptr<Light>> lights;
    auto reader = global::manager.GetReader(SceneManager::Type::Lights);

    while (!reader->eof()) {
        protobuf::Light proto_light;
        reader->read(&proto_light);
        lights.push_back(move(light::from_protobuf(proto_light)));
    }

    return lights;
}

shared_ptr<Sampler> loadSampler(const string &scenePath) {
    auto reader = global::manager.GetReader(SceneManager::Type::Sampler);
    protobuf::Sampler proto_sampler;
    reader->read(&proto_sampler);
    return sampler::from_protobuf(proto_sampler);
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

        const string raysPath{argv[1]};
        const string scenePath{argv[2]};
        const string outputPath{argv[3]};
        const string finishedPath{argv[4]};

        global::manager.init(scenePath);

        vector<RayState> rayStates;
        vector<RayState> outputRays;
        vector<RayState> finishedRays;

        /* loading all the rays */
        {
            protobuf::RecordReader reader{raysPath};
            while (!reader.eof()) {
                protobuf::RayState protoState;
                if (!reader.read(&protoState)) {
                    continue;
                }

                rayStates.push_back(move(from_protobuf(protoState)));
            }
        }

        cerr << rayStates.size() << " RayState(s) loaded." << endl;

        if (!rayStates.size()) {
            return EXIT_SUCCESS;
        }

        MemoryArena arena;
        auto treelet = make_shared<CloudBVH>();
        auto lights = loadLights(scenePath);
        auto sampler = loadSampler(scenePath);

        for (auto &rayState : rayStates) {
            if (not rayState.toVisit.empty()) {
                auto newRay = CloudIntegrator::Trace(move(rayState), treelet);
                if (!newRay.isShadowRay || !newRay.hit.initialized()) {
                    outputRays.push_back(move(newRay));
                }
            } else if (rayState.isShadowRay && !rayState.hit.initialized()) {
                finishedRays.push_back(move(rayState));
            } else if (rayState.hit.initialized()) {
                auto newRays = CloudIntegrator::Shade(move(rayState), treelet,
                                                      lights, sampler, arena);
                for (auto &newRay : newRays) {
                    outputRays.push_back(move(newRay));
                }
            }
        }

        /* writing all the output rays */
        {
            protobuf::RecordWriter writer{outputPath};
            for (auto &rayState : outputRays) {
                writer.write(to_protobuf(rayState));
            }
        }

        /* writing all the finished rays */
        {
            protobuf::RecordWriter writer{finishedPath};
            for (auto &rayState : finishedRays) {
                writer.write(to_protobuf(rayState));
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
