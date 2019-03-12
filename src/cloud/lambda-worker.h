#ifndef PBRT_CLOUD_LAMBDA_WORKER_H
#define PBRT_CLOUD_LAMBDA_WORKER_H

#include <cstring>
#include <deque>
#include <fstream>
#include <iostream>
#include <string>

#include "cloud/bvh.h"
#include "cloud/lambda-master.h"
#include "cloud/lambda.h"
#include "cloud/raystate.h"
#include "cloud/stats.h"
#include "core/camera.h"
#include "core/geometry.h"
#include "core/light.h"
#include "core/sampler.h"
#include "core/transform.h"
#include "execution/loop.h"
#include "execution/meow/message.h"
#include "net/address.h"
#include "storage/backend.h"
#include "util/seq_no_set.h"
#include "util/temp_dir.h"
#include "util/timerfd.h"

namespace pbrt {

class LambdaWorker {
  public:
    LambdaWorker(const std::string& coordinatorIP,
                 const uint16_t coordinatorPort,
                 const std::string& storageBackendUri, const bool sendReliably,
                 const int samplesPerPixel,
                 const FinishedRayAction finishedRayAction,
                 const float raysLogRate);

    void run();
    void terminate() { terminated = true; }
    void uploadLogs();

  private:
    struct Worker {
        enum class State { Connecting, Connected };

        WorkerId id;
        Address address;
        State state{State::Connecting};
        int32_t seed{0};
        uint32_t tries{0};

        std::set<TreeletId> treelets{};

        Worker(const WorkerId id, Address&& addr)
            : id(id), address(std::move(addr)) {}
    };

    struct RayPacket {
        Address destination;
        TreeletId targetTreelet;
        size_t rayCount;
        std::string data;

        bool reliable{false};
        uint64_t sequenceNumber;

        RayPacket(const Address& addr, const TreeletId targetTreelet,
                  const size_t rayCount, std::string&& data,
                  const bool reliable = false,
                  const uint64_t sequenceNumber = 0)
            : destination(addr),
              targetTreelet(targetTreelet),
              rayCount(rayCount),
              data(std::move(data)),
              reliable(reliable),
              sequenceNumber(sequenceNumber) {}
    };

    bool processMessage(const meow::Message& message);
    void initializeScene();

    void loadCamera();
    void loadSampler();
    void loadLights();
    void loadFakeScene();

    Poller::Action::Result::Type handleRayQueue();
    Poller::Action::Result::Type handleOutQueue();
    Poller::Action::Result::Type handleFinishedQueue();
    Poller::Action::Result::Type handlePeers();
    Poller::Action::Result::Type handleMessages();
    Poller::Action::Result::Type handleNeededTreelets();

    Poller::Action::Result::Type handleUdpSend();
    Poller::Action::Result::Type handleUdpReceive();
    Poller::Action::Result::Type handleRayAcknowledgements();

    Poller::Action::Result::Type handleWorkerStats();
    Poller::Action::Result::Type handleDiagnostics();

    meow::Message createConnectionRequest(const Worker& peer);
    meow::Message createConnectionResponse(const Worker& peer);

    void generateRays(const Bounds2i& cropWindow);
    void getObjects(const protobuf::GetObjects& objects);

    void pushRayQueue(RayStatePtr&& state);
    RayStatePtr popRayQueue();

    /* Logging & Diagnostics */
    const std::string logBase{"pbrt-worker"};
    const std::string infoLogName{logBase + ".INFO"};
    const std::string diagnosticsName{logBase + ".DIAG"};
    const std::string logPrefix{"logs/"};
    std::ofstream diagnosticsOstream{};
    const float raysLogRate;
    const bool trackRays{raysLogRate > 0};

    WorkerDiagnostics lastDiagnostics;

    const bool sendReliably;
    const Address coordinatorAddr;
    const UniqueDirectory workingDirectory;
    ExecutionLoop loop{};
    std::unique_ptr<StorageBackend> storageBackend;
    std::shared_ptr<TCPConnection> coordinatorConnection;
    meow::MessageParser messageParser{};
    meow::MessageParser tcpMessageParser{};
    Optional<WorkerId> workerId;
    std::map<WorkerId, Worker> peers{};
    int32_t mySeed;
    bool peerRequested{false};
    std::string outputName;
    const FinishedRayAction finishedRayAction;

    /* Sending rays to other nodes */
    using packet_clock = std::chrono::steady_clock;

    UDPConnection udpConnection{true};
    std::deque<std::pair<Address, std::string>> servicePackets{};

    /* outgoing rays */
    std::deque<RayPacket> rayPackets{};
    std::deque<std::pair<packet_clock::time_point, RayPacket>>
        outstandingRayPackets{};
    std::map<Address, SeqNoSet> receivedAcks{};
    std::map<Address, uint64_t> sequenceNumbers{};

    /* incoming rays */
    std::map<Address, SeqNoSet> receivedPacketSeqNos{};
    std::map<Address, std::vector<uint64_t>> toBeAcked{};

    /* Scene Data */
    bool initialized{false};
    int samplesPerPixel{0};
    std::vector<std::unique_ptr<Transform>> transformCache{};
    std::shared_ptr<Camera> camera{};
    std::unique_ptr<FilmTile> filmTile{};
    std::shared_ptr<Sampler> sampler{};
    std::unique_ptr<Scene> fakeScene{};
    std::vector<std::shared_ptr<Light>> lights{};
    std::shared_ptr<CloudBVH> bvh;
    std::set<uint32_t> treeletIds{};
    MemoryArena arena;

    /* Rays */
    std::deque<RayStatePtr> rayQueue{};
    std::deque<RayStatePtr> finishedQueue{};
    std::map<TreeletId, std::deque<RayStatePtr>> pendingQueue{};
    std::map<TreeletId, std::deque<RayStatePtr>> outQueue{};
    size_t pendingQueueSize{0};
    size_t outQueueSize{0};

    std::map<TreeletId, std::vector<WorkerId>> treeletToWorker{};
    std::set<TreeletId> neededTreelets{};
    std::set<TreeletId> requestedTreelets{};

    /* Always-on FD */
    FileDescriptor dummyFD{STDOUT_FILENO};

    /* Timers */
    TimerFD peerTimer;
    TimerFD workerStatsTimer;
    TimerFD workerDiagnosticsTimer;
    TimerFD handleRayAcknowledgementsTimer;

    bool terminated{false};
};

}  // namespace pbrt

#endif /* PBRT_CLOUD_LAMBDA_WORKER_H */
