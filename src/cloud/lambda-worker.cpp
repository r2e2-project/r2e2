#include <cstdlib>
#include <deque>
#include <iostream>
#include <limits>
#include <sstream>
#include <stdexcept>

#include "cloud/manager.h"
#include "cloud/raystate.h"
#include "core/camera.h"
#include "core/geometry.h"
#include "core/light.h"
#include "core/sampler.h"
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

class ProgramFinished : public exception {};

void usage(const char* argv0) {
    cerr << "Usage: " << argv0 << " DESTINATION PORT STORAGE-BACKEND" << endl;
}

struct Peer {
    enum class State { Connecting, Connected };

    Address address;
    State state{State::Connecting};
    int32_t seed{0};

    Peer(Address&& addr) : address(move(addr)) {}
};

class LambdaWorker {
  public:
    LambdaWorker(const string& coordinatorIP, const uint16_t coordinatorPort,
                 const string& storageBackendUri);

    void run();

  private:
    bool process_message(const Message& message);
    void initializeScene();

    void loadCamera();
    void loadSampler();
    void loadLights();
    void loadFakeScene();

    Address coordinatorAddr;
    ExecutionLoop loop{};
    UniqueDirectory workingDirectory;
    unique_ptr<StorageBackend> storageBackend;
    shared_ptr<TCPConnection> coordinatorConnection;
    shared_ptr<UDPConnection> udpConnection;
    MessageParser messageParser{};
    Optional<size_t> workerId;
    map<size_t, Peer> peers;
    int32_t mySeed;

    /* Scene Data */
    vector<unique_ptr<Transform>> transformCache{};
    bool initialized{false};
    shared_ptr<Camera> camera{};
    shared_ptr<Sampler> sampler{};
    unique_ptr<Scene> fakeScene{};
    vector<shared_ptr<Light>> lights{};
    deque<RayState> rayQueue{};
};

LambdaWorker::LambdaWorker(const string& coordinatorIP,
                           const uint16_t coordinatorPort,
                           const string& storageBackendUri)
    : coordinatorAddr(coordinatorIP, coordinatorPort),
      workingDirectory("/tmp/pbrt-worker"),
      storageBackend(StorageBackend::create_backend(storageBackendUri)) {
    roost::chdir(workingDirectory.name());
    global::manager.init(".");

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

    Message helloMsg{Message::OpCode::Hey, ""};
    coordinatorConnection->enqueue_write(helloMsg.str());
}

void LambdaWorker::run() {
    constexpr int TIMEOUT = 500; /* ms */
    while (true) {
        auto pollerResult = loop.loop_once(TIMEOUT).result;
        if (pollerResult != Poller::Result::Type::Success &&
            pollerResult != Poller::Result::Type::Timeout) {
            break;
        }

        while (!messageParser.empty()) {
            Message message = move(messageParser.front());
            messageParser.pop();
            if (!process_message(message)) {
                messageParser.push(move(message));
            }
        }

        for (auto& kv : peers) {
            auto& peerId = kv.first;
            auto& peer = kv.second;

            switch (peer.state) {
            case Peer::State::Connecting: {
                protobuf::ConnectRequest connectRequestProto;
                connectRequestProto.set_worker_id(*workerId);
                connectRequestProto.set_my_seed(mySeed);
                connectRequestProto.set_your_seed(peer.seed);

                Message connectRequestMessage{
                    Message::OpCode::ConnectionRequest,
                    protoutil::to_string(connectRequestProto)};

                udpConnection->enqueue_datagram(peer.address,
                                                connectRequestMessage.str());

                break;
            }

            case Peer::State::Connected:
                /* send keep alive */
                break;
            }
        }
    }
}

bool LambdaWorker::process_message(const Message& message) {
    switch (message.opcode()) {
    case Message::OpCode::Hey:
        workerId.reset(stoi(message.payload()));
        udpConnection->enqueue_datagram(coordinatorAddr, to_string(*workerId));
        break;

    case Message::OpCode::Ping: {
        Message pong{Message::OpCode::Pong, ""};
        coordinatorConnection->enqueue_write(pong.str());
        break;
    }

    case Message::OpCode::Get: {
        vector<storage::GetRequest> getRequests;

        string line;
        istringstream iss{message.payload()};

        while (getline(iss, line)) {
            if (line.length()) getRequests.emplace_back(line, line);
        }

        storageBackend->get(getRequests);
        break;
    }

    case Message::OpCode::GenerateRays: {
        protobuf::GenerateRays data;
        protoutil::from_string(message.payload(), data);

        const auto& bounds = from_protobuf(data.crop_window());
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

        break;
    }

    case Message::OpCode::ConnectTo: {
        protobuf::ConnectTo connectProto;
        protoutil::from_string(message.payload(), connectProto);

        if (peers.count(connectProto.worker_id())) {
            /* We already have this peer (do something?) */
            break;
        }

        const auto destination = Address::decompose(connectProto.address());
        peers.emplace(
            piecewise_construct, forward_as_tuple(connectProto.worker_id()),
            forward_as_tuple(Address{destination.first, destination.second}));
        break;
    }

    case Message::OpCode::ConnectionRequest: {
        protobuf::ConnectRequest connectRequestProto;
        protoutil::from_string(message.payload(), connectRequestProto);

        const auto otherWorkerId = connectRequestProto.worker_id();

        if (peers.count(otherWorkerId) == 0) {
            /* we still haven't heard about this worker id from the master,
            we should wait. */
            return false;
        }

        auto& peer = peers.at(otherWorkerId);

        if (peer.state == Peer::State::Connected) {
            break;
        }

        peer.seed = connectRequestProto.my_seed();

        if (connectRequestProto.your_seed() == mySeed) {
            peer.state = Peer::State::Connected;
            cerr << "connected to worker " << otherWorkerId << endl;
        }

        break;
    }

    case Message::OpCode::Bye:
        throw ProgramFinished();
        break;

    default:
        throw runtime_error("unhandled message opcode");
    }

    return true;
}

void LambdaWorker::loadCamera() {
    auto reader = global::manager.GetReader(SceneManager::Type::Camera);
    protobuf::Camera proto_camera;
    reader->read(&proto_camera);
    camera = camera::from_protobuf(proto_camera, transformCache);
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
