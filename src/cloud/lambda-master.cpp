#include <deque>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "cloud/manager.h"
#include "cloud/raystate.h"
#include "core/camera.h"
#include "core/geometry.h"
#include "core/transform.h"
#include "execution/loop.hh"
#include "execution/meow/message.hh"
#include "messages/utils.h"
#include "net/socket.hh"
#include "util/exception.h"

using namespace std;
using namespace pbrt;
using namespace meow;

shared_ptr<Camera> loadCamera(const string &scenePath,
                              vector<unique_ptr<Transform>> &transformCache) {
    auto reader = global::manager.GetReader(SceneManager::Type::Camera);
    protobuf::Camera proto_camera;
    reader->read(&proto_camera);
    return camera::from_protobuf(proto_camera, transformCache);
}

shared_ptr<Sampler> loadSampler(const string &scenePath) {
    auto reader = global::manager.GetReader(SceneManager::Type::Sampler);
    protobuf::Sampler proto_sampler;
    reader->read(&proto_sampler);
    return sampler::from_protobuf(proto_sampler);
}

void usage(const char *argv0) { cerr << argv0 << " SCENE-DATA PORT" << endl; }

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
        const uint16_t listenPort = stoi(argv[2]);

        global::manager.init(scenePath);

        vector<unique_ptr<Transform>> transformCache;
        shared_ptr<Sampler> sampler = loadSampler(scenePath);
        shared_ptr<Camera> camera = loadCamera(scenePath, transformCache);

        const Bounds2i sampleBounds = camera->film->GetSampleBounds();
        const uint8_t maxDepth = 5;
        const float rayScale = 1 / sqrt((Float)sampler->samplesPerPixel);

        deque<RayState> rayStates;
        vector<CloudIntegrator::SampleData> cameraSamples;

        /* Generate all the samples */
        size_t i = 0;
        for (Point2i pixel : sampleBounds) {
            sampler->StartPixel(pixel);

            if (!InsideExclusive(pixel, sampleBounds)) continue;

            size_t sample_num = 0;
            do {
                CloudIntegrator::SampleData sampleData;
                sampleData.sample = sampler->GetCameraSample(pixel);

                RayState state;
                state.sample.id = i++;
                state.sample.num = sample_num++;
                state.sample.pixel = pixel;
                state.remainingBounces = maxDepth;
                sampleData.weight = camera->GenerateRayDifferential(
                    sampleData.sample, &state.ray);
                state.ray.ScaleDifferentials(rayScale);
                state.StartTrace();

                rayStates.push_back(move(state));
                cameraSamples.push_back(move(sampleData));
            } while (sampler->StartNextSample());
        }

        cout << rayStates.size() << " samples generated." << endl;

        struct Lambda {
            enum class State { Idle, Busy };
            size_t id;
            State state{State::Idle};
            shared_ptr<TCPConnection> connection;

            Lambda(const size_t id, shared_ptr<TCPConnection> &&connection)
                : id(id), connection(move(connection)) {}
        };

        ExecutionLoop loop;
        uint64_t current_lambda_id = 0;
        map<uint64_t, Lambda> lambdas;
        set<uint64_t> freeLambdas;

        loop.make_listener(
            {"0.0.0.0", listenPort},
            [&current_lambda_id, &lambdas, &freeLambdas](ExecutionLoop &loop,
                                                         TCPSocket &&socket) {
                cerr << "Incoming connection from "
                     << socket.peer_address().str() << endl;

                auto messageParser = make_shared<MessageParser>();
                auto connection = loop.add_connection<TCPSocket>(
                    move(socket),
                    [messageParser](shared_ptr<TCPConnection>, string &&data) {
                        messageParser->parse(data);

                        while (!messageParser->empty()) {
                            Message message = move(messageParser->front());
                            messageParser->pop();

                            cerr << "message(" << (int)message.opcode() << ")\n"
                                 << endl;
                        }

                        return true;
                    },
                    []() { throw runtime_error("error occured"); },
                    []() { throw runtime_error("worker died"); });

                lambdas.emplace(
                    piecewise_construct, forward_as_tuple(current_lambda_id),
                    forward_as_tuple(current_lambda_id, move(connection)));
                freeLambdas.insert(current_lambda_id);
                current_lambda_id++;

                return true;
            });

        while (true) {
            loop.loop_once(-1);
        }
    } catch (const exception &e) {
        print_exception(argv[0], e);
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
