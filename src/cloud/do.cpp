#include <iostream>
#include <string>

#include "cloud/bvh.h"
#include "cloud/integrator.h"
#include "cloud/raystate.h"
#include "messages/serialization.h"
#include "messages/utils.h"
#include "util/exception.h"

using namespace std;
using namespace pbrt;

void usage(const char *argv0) {
    cerr << argv0 << " OP=<trace|shade> RAYSTATES SCENE-DATA OUTPUT" << endl;
}

vector<shared_ptr<Light>> loadLights(const string &scenePath) {
    vector<shared_ptr<Light>> lights;
    protobuf::RecordReader reader{scenePath + "/LIGHTS"};

    while (!reader.eof()) {
        protobuf::Light proto_light;
        reader.read(&proto_light);
        lights.push_back(move(from_protobuf(proto_light)));
    }

    return lights;
}

shared_ptr<Sampler> loadSampler(const string &scenePath) {
    shared_ptr<Sampler> sampler;
    protobuf::RecordReader reader{scenePath + "/SAMPLER"};

    protobuf::Sampler proto_sampler;
    reader.read(&proto_sampler);
    return from_protobuf(proto_sampler);
}

enum class Operation { Trace, Shade };

int main(int argc, char const *argv[]) {
    try {
        if (argc <= 0) {
            abort();
        }

        if (argc != 4) {
            usage(argv[0]);
            return EXIT_FAILURE;
        }

        const string operationStr{argv[1]};
        const string raysPath{argv[2]};
        const string scenePath{argv[3]};
        const string output{argv[4]};

        Operation operation;
        if (operationStr == "trace") {
            operation = Operation::Trace;
        } else if (operationStr == "shade") {
            operation = Operation::Shade;
        } else {
            throw runtime_error("invalid operation mode");
        }

        vector<RayState> rayStates;
        vector<RayState> outputRays;

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

        switch (operation) {
        case Operation::Trace: {
            auto treelet = make_shared<CloudBVH>(
                scenePath, rayStates.front().toVisit.back().treelet);

            for (auto &rayState : rayStates) {
                auto newRay = CloudIntegrator::Trace(move(rayState), treelet);
                if (!newRay.isShadowRay || !newRay.hit.initialized()) {
                    outputRays.push_back(move(newRay));
                }
            }
            break;
        }

        case Operation::Shade: {
            auto treelet = make_shared<CloudBVH>(
                scenePath, rayStates.front().hit->treelet);

            MemoryArena arena;
            auto lights = loadLights(scenePath);
            auto sampler = loadSampler(scenePath);

            for (auto &rayState : rayStates) {
                auto newRay = CloudIntegrator::Shade(move(rayState), treelet,
                                                     lights, sampler, arena);
            }

            break;
        }
        }

        /* writing all the output rays */
        {
            protobuf::RecordWriter writer{output};
            for (auto &rayState : outputRays) {
                writer.write(to_protobuf(rayState));
            }
        }

        cerr << outputRays.size() << " RayState(s) written." << endl;
    } catch (const exception &e) {
        print_exception(argv[0], e);
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
