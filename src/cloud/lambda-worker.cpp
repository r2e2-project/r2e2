#include "lambda-worker.h"

#include <cstdlib>
#include <limits>
#include <sstream>
#include <stdexcept>

#include "cloud/bvh.h"
#include "cloud/integrator.h"
#include "cloud/manager.h"
#include "cloud/raystate.h"
#include "core/camera.h"
#include "core/geometry.h"
#include "core/light.h"
#include "core/sampler.h"
#include "core/spectrum.h"
#include "core/transform.h"
#include "execution/loop.h"
#include "execution/meow/message.h"
#include "messages/utils.h"
#include "net/address.h"
#include "net/requests.h"
#include "storage/backend.h"
#include "util/exception.h"
#include "util/path.h"
#include "util/system_runner.h"
#include "util/temp_dir.h"
#include "util/temp_file.h"

using namespace std;
using namespace meow;
using namespace pbrt;

using OpCode = Message::OpCode;
using PollerResult = Poller::Result::Type;

class ProgramFinished : public exception {};

void usage(const char* argv0) {
    cerr << "Usage: " << argv0 << " DESTINATION PORT STORAGE-BACKEND" << endl;
}

LambdaWorker::LambdaWorker(const string& coordinatorIP,
                           const uint16_t coordinatorPort,
                           const string& storageBackendUri)
    : coordinatorAddr(coordinatorIP, coordinatorPort),
      workingDirectory("/tmp/pbrt-worker"),
      storageBackend(StorageBackend::create_backend(storageBackendUri)) {
    cerr << "* starting worker in " << workingDirectory.name() << endl;
    PbrtOptions.nThreads = 1;
    roost::chdir(workingDirectory.name());
    global::manager.init(".");

    treelet = make_shared<CloudBVH>();

    srand(time(nullptr));
    do {
        mySeed = rand();
    } while (mySeed == 0);

    coordinatorConnection = loop.make_connection<TCPConnection>(
        coordinatorAddr,
        [this](shared_ptr<TCPConnection>, string&& data) {
            this->messageParser.parse(data);
            return true;
        },
        []() { cerr << "Error." << endl; }, []() { throw ProgramFinished(); });

    udpConnection = loop.make_udp_connection(
        [this](shared_ptr<UDPConnection>, Address&& addr, string&& data) {
            this->messageParser.parse(data);
            return true;
        },
        []() { cerr << "Error." << endl; }, []() { throw ProgramFinished(); });

    Message message{OpCode::Hey, ""};
    coordinatorConnection->enqueue_write(message.str());
}

void LambdaWorker::generateRays(const Bounds2i& bounds) {
    initializeScene();

    const Bounds2i sampleBounds = camera->film->GetSampleBounds();
    const uint8_t maxDepth = 5;
    const float rayScale = 1 / sqrt((Float)sampler->samplesPerPixel);

    for (const Point2i pixel : bounds) {
        sampler->StartPixel(pixel);
        if (!InsideExclusive(pixel, sampleBounds)) continue;

        size_t sampleNum = 0;
        do {
            CameraSample cameraSample = sampler->GetCameraSample(pixel);

            RayState state;
            state.sample.num = sampleNum++;
            state.sample.pixel = pixel;
            state.sample.pFilm = cameraSample.pFilm;
            state.sample.weight =
                camera->GenerateRayDifferential(cameraSample, &state.ray);
            state.remainingBounces = maxDepth;
            state.ray.ScaleDifferentials(rayScale);
            state.StartTrace();

            rayQueue.push_back(move(state));
        } while (sampler->StartNextSample());
    }
}

bool LambdaWorker::processMessage(const Message& message) {
    cerr << ">> [msg:" << Message::OPCODE_NAMES[to_underlying(message.opcode())]
         << "]\n";

    switch (message.opcode()) {
    case OpCode::Hey:
        workerId.reset(stoull(message.payload()));
        udpConnection->enqueue_datagram(coordinatorAddr, to_string(*workerId));
        break;

    case OpCode::Ping: {
        Message pong{OpCode::Pong, ""};
        coordinatorConnection->enqueue_write(pong.str());
        break;
    }

    case OpCode::Get: {
        string line;
        istringstream iss{message.payload()};
        vector<storage::GetRequest> requests;
        while (getline(iss, line)) {
            if (line.length()) requests.emplace_back(line, line);
        }

        storageBackend->get(requests);
        break;
    }

    case OpCode::GenerateRays: {
        protobuf::GenerateRays proto;
        protoutil::from_string(message.payload(), proto);
        generateRays(from_protobuf(proto.crop_window()));
        break;
    }

    case OpCode::ConnectTo: {
        protobuf::ConnectTo proto;
        protoutil::from_string(message.payload(), proto);

        if (peers.count(proto.worker_id()) == 0) {
            const auto dest = Address::decompose(proto.address());
            peers.emplace(proto.worker_id(), Peer{{dest.first, dest.second}});
        }

        break;
    }

    case OpCode::ConnectionRequest: {
        protobuf::ConnectRequest proto;
        protoutil::from_string(message.payload(), proto);

        const auto otherWorkerId = proto.worker_id();
        if (peers.count(otherWorkerId) == 0) {
            /* we still haven't heard about this worker id from the master,
            we should wait. */
            return false;
        }

        auto& peer = peers.at(otherWorkerId);
        if (peer.state == Peer::State::Connected) {
            break;
        }

        peer.seed = proto.my_seed();
        if (proto.your_seed() == mySeed) {
            peer.state = Peer::State::Connected;
            cerr << "connected to worker " << otherWorkerId << endl;
        }

        break;
    }

    case OpCode::SendRays: {
        protobuf::RecordReader reader{istringstream{message.payload()}};
        protobuf::RayState proto;

        while (!reader.eof()) {
            if (reader.read(&proto)) {
                rayQueue.push_back(move(from_protobuf(proto)));
            }
        }

        break;
    }

    case OpCode::Bye:
        throw ProgramFinished();
        break;

    default:
        throw runtime_error("unhandled message opcode");
    }

    return true;
}

void LambdaWorker::run() {
    constexpr int TIMEOUT = 500; /* ms */
    while (true) {
        auto res = loop.loop_once(TIMEOUT).result;
        if (res != PollerResult::Success && res != PollerResult::Timeout) {
            break;
        }

        /* process received messages (messages that can't be processed yet
         * are ignored for this iteration) */
        deque<Message> unprocessedMessages;
        while (!messageParser.empty()) {
            Message message = move(messageParser.front());
            messageParser.pop();

            if (!processMessage(message)) {
                unprocessedMessages.push_back(move(message));
            }
        }

        while (!unprocessedMessages.empty()) {
            messageParser.push(move(unprocessedMessages.front()));
            unprocessedMessages.pop_front();
        }

        /* ensure peer connections are healthy */
        for (auto& kv : peers) {
            auto& peerId = kv.first;
            auto& peer = kv.second;

            switch (peer.state) {
            case Peer::State::Connecting: {
                protobuf::ConnectRequest proto;
                proto.set_worker_id(*workerId);
                proto.set_my_seed(mySeed);
                proto.set_your_seed(peer.seed);
                Message message{OpCode::ConnectionRequest,
                                protoutil::to_string(proto)};

                udpConnection->enqueue_datagram(peer.address, message.str());
                break;
            }

            case Peer::State::Connected:
                /* send keep alive */
                break;
            }
        }

        /* let's trace rays that we have to trace */
        cerr << ">>> tracing " << rayQueue.size() << " ray"
             << ((rayQueue.size() == 1) ? "" : "s") << endl;

        while (!rayQueue.empty()) {
            RayState ray = move(rayQueue.front());
            rayQueue.pop_front();

            /* the ray is still tracing through the bvh, so continue tracing it */
            if (!ray.toVisit.empty()) {
                auto newRay = CloudIntegrator::Trace(move(ray), treelet);
                if (!newRay.isShadowRay || !newRay.hit.initialized()) {
                    processedRays.push_back(move(newRay));
                }
            } else if (ray.isShadowRay) {
                if (!ray.hit.initialized()) {
                  /* the shadow ray didn't hit anything, so count this ray's contribution*/
                    finishedRays.push_back(move(ray));
                }
            /* the ray hit something, so shade it */
            } else if (ray.hit.initialized()) {
                auto newRays = CloudIntegrator::Shade(move(ray), treelet,
                                                      lights, sampler, arena);
                for (auto& newRay : newRays) {
                    processedRays.push_back(move(newRay));
                }
            }
        }

        /* check if a ray should be pushed to another worker or traced locally
         */
        while (!processedRays.empty()) {
            RayState ray = move(processedRays.front());
            processedRays.pop_front();

            if (ray.currentTreelet() % 2 == *workerId % 2) {
                rayQueue.push_back(move(ray));
            } else {
                outQueue.push_back(move(ray));
            }
        }

        /* send rays to peers */
        if (!outQueue.empty()) {
            if (peers.size() == 0 && !peerRequested) {
                Message message{OpCode::GetWorker, ""};
                coordinatorConnection->enqueue_write(message.str());
                peerRequested = true;
            } else if (peers.size()) {
                auto& peer = peers.begin()->second;

                if (peer.state == Peer::State::Connected) {
                    while (!outQueue.empty()) {
                        ostringstream oss;
                        protobuf::RecordWriter writer{&oss};

                        while (oss.tellp() < 1'000 && !outQueue.empty()) {
                            RayState ray = move(outQueue.front());
                            outQueue.pop_front();
                            writer.write(to_protobuf(ray));
                        }

                        Message message{OpCode::SendRays, oss.str()};
                        udpConnection->enqueue_datagram(peer.address,
                                                        message.str());
                    }
                }
            }
        }

        /* aggregate film updates from finished rays into the local filmTile */
        while (!finishedRays.empty()) {
            RayState ray = move(finishedRays.front());
            finishedRays.pop_front();

            Spectrum L{ray.Ld * ray.beta};
            if (L.HasNaNs() || L.y() < -1e-5 || isinf(L.y())) {
                L = Spectrum(0.f);
            }

            filmTile->AddSample(ray.sample.pFilm, L, ray.sample.weight);
        }
    }
}

void LambdaWorker::loadCamera() {
    auto reader = global::manager.GetReader(SceneManager::Type::Camera);
    protobuf::Camera proto_camera;
    reader->read(&proto_camera);
    camera = camera::from_protobuf(proto_camera, transformCache);
    filmTile = camera->film->GetFilmTile(camera->film->GetSampleBounds());
}

void LambdaWorker::loadSampler() {
    auto reader = global::manager.GetReader(SceneManager::Type::Sampler);
    protobuf::Sampler proto_sampler;
    reader->read(&proto_sampler);
    sampler = sampler::from_protobuf(proto_sampler);
}

void LambdaWorker::loadLights() {
    auto reader = global::manager.GetReader(SceneManager::Type::Lights);
    while (!reader->eof()) {
        protobuf::Light proto_light;
        reader->read(&proto_light);
        lights.push_back(move(light::from_protobuf(proto_light)));
    }
}

void LambdaWorker::loadFakeScene() {
    auto reader = global::manager.GetReader(SceneManager::Type::Scene);
    protobuf::Scene proto_scene;
    reader->read(&proto_scene);
    fakeScene = make_unique<Scene>(from_protobuf(proto_scene));
}

void LambdaWorker::initializeScene() {
    if (initialized) return;

    loadCamera();
    loadSampler();
    loadLights();
    loadFakeScene();

    for (auto& light : lights) {
        light->Preprocess(*fakeScene);
    }

    initialized = true;
}

int main(int argc, char const* argv[]) {
    try {
        if (argc <= 0) {
            abort();
        }

        if (argc != 4) {
            usage(argv[0]);
            return EXIT_FAILURE;
        }

        google::InitGoogleLogging(argv[0]);

        const uint16_t coordinatorPort = stoi(argv[2]);

        LambdaWorker worker{argv[1], coordinatorPort, argv[3]};
        worker.run();
    } catch (const ProgramFinished&) {
        return EXIT_SUCCESS;
    } catch (const exception& e) {
        print_exception(argv[0], e);
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
