#ifndef PBRT_CLOUD_LAMBDA_WORKER_H
#define PBRT_CLOUD_LAMBDA_WORKER_H

#include <cstring>
#include <deque>
#include <fstream>
#include <iostream>
#include <random>
#include <string>
#include <tuple>

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

constexpr std::chrono::milliseconds PEER_CHECK_INTERVAL{250};
constexpr std::chrono::milliseconds HANDLE_ACKS_INTERVAL{50};
constexpr std::chrono::milliseconds OUT_QUEUE_INTERVAL{100};
constexpr std::chrono::milliseconds WORKER_STATS_INTERVAL{1'000};
constexpr std::chrono::milliseconds WORKER_DIAGNOSTICS_INTERVAL{2'000};
constexpr std::chrono::milliseconds KEEP_ALIVE_INTERVAL{40'000};
constexpr std::chrono::milliseconds FINISHED_PATHS_INTERVAL{2'500};
constexpr std::chrono::milliseconds PACKET_TIMEOUT{100};

struct WorkerConfiguration {
    bool sendReliably;
    uint64_t maxUdpRate;
    int samplesPerPixel;
    FinishedRayAction finishedRayAction;
    float rayActionsLogRate;
    float packetsLogRate;
};

class LambdaWorker {
  public:
    LambdaWorker(const std::string& coordinatorIP,
                 const uint16_t coordinatorPort,
                 const std::string& storageBackendUri,
                 const WorkerConfiguration& config);

    void run();
    void terminate() { terminated = true; }
    void uploadLogs();

  private:
    using packet_clock = std::chrono::steady_clock;
    using rays_clock = std::chrono::system_clock;

    struct Worker {
        enum class State { Connecting, Connected };

        WorkerId id;
        Address address;
        State state{State::Connecting};
        packet_clock::time_point nextKeepAlive{};
        int32_t seed{0};
        uint32_t tries{0};

        std::set<TreeletId> treelets{};

        Worker(const WorkerId id, Address&& addr)
            : id(id), address(std::move(addr)) {}
    };

    struct ServicePacket {
        Address destination;
        WorkerId destinationId;
        std::string data;
        bool ackPacket;
        uint64_t ackId;
        bool tracked;

        ServicePacket(const Address& addr, const WorkerId destId,
                      std::string&& data, const bool ackPacket = false,
                      const uint64_t ackId = 0, const bool tracked = false)
            : destination(addr),
              destinationId(destId),
              data(move(data)),
              ackPacket(ackPacket),
              ackId(ackId),
              tracked(tracked) {}
    };

    struct RayPacket {
        Address destination;
        WorkerId destinationId;
        TreeletId targetTreelet;
        size_t rayCount;

        bool retransmission{false};
        bool reliable{false};
        bool tracked{false};
        uint64_t sequenceNumber;
        size_t attempt{0};

        std::vector<std::unique_ptr<RayState>> trackedRays;

        std::string& data() { return data_; }

        RayPacket(const Address& addr, const WorkerId destId,
                  const TreeletId targetTreelet, const size_t rayCount,
                  std::string&& data, const bool reliable = false,
                  const uint64_t sequenceNumber = 0, const bool tracked = false)
            : destination(addr),
              destinationId(destId),
              targetTreelet(targetTreelet),
              rayCount(rayCount),
              data_(std::move(data)),
              reliable(reliable),
              sequenceNumber(sequenceNumber),
              tracked(tracked) {}

        void incrementAttempts() {
            if (data_.length() < 2) return;

            attempt++;
            const uint16_t val = htobe16(attempt);
            memcpy(&data_[0], reinterpret_cast<const char*>(&val), sizeof(val));
        }

      private:
        mutable std::string data_;
    };

    enum class RayAction {
        Generated,
        Traced,
        Pending,
        Queued,
        Sent,
        Received,
        Finished
    };

    enum class PacketAction {
        Queued,
        Sent,
        Received,
        AckSent,
        AckReceived,
        Acked
    };

    enum class Event {
        RayQueue,
        OutQueue,
        FinishedQueue,
        FinishedPaths,
        Peers,
        Messages,
        NeededTreelets,
        UdpSend,
        UdpReceive,
        RayAcks,
        WorkerStats,
        Diagnostics,
        NetStats,
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
    Poller::Action::Result::Type handleFinishedPaths();
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

    void logRayAction(const RayState& state, const RayAction action,
                      const WorkerId otherParty = -1);

    void logPacket(const uint64_t sequenceNumber, const uint16_t attempt,
                   const PacketAction action, const WorkerId otherParty,
                   const size_t packetSize, const size_t numRays = 0);

    void initBenchmark(const uint32_t duration, const uint32_t destination,
                       const uint32_t rate);

    ////////////////////////////////////////////////////////////////////////////
    // MEMBER VARIABLES                                                       //
    ////////////////////////////////////////////////////////////////////////////

    const WorkerConfiguration config;

    /* Logging & Diagnostics */
    const std::string logBase{"pbrt-worker"};
    const std::string infoLogName{logBase + ".INFO"};
    std::string logPrefix{"logs/"};
    const bool trackRays{config.rayActionsLogRate > 0};
    const bool trackPackets{config.packetsLogRate > 0};

    std::mt19937 randEngine{std::random_device{}()};
    std::bernoulli_distribution packetLogBD{config.packetsLogRate};

    WorkerStats workerStats;
    WorkerDiagnostics lastDiagnostics;

    const Address coordinatorAddr;
    const UniqueDirectory workingDirectory;
    ExecutionLoop loop{};
    std::map<Event, uint64_t> eventAction{};
    std::unique_ptr<StorageBackend> storageBackend;
    std::shared_ptr<TCPConnection> coordinatorConnection;
    meow::MessageParser messageParser{};
    meow::MessageParser tcpMessageParser{};
    Optional<WorkerId> workerId;
    Optional<std::string> jobId;
    std::map<WorkerId, Worker> peers{};
    std::map<Address, WorkerId> addressToWorker{};
    int32_t mySeed;
    std::string outputName;

    /* Sending rays to other nodes */
    uint64_t ackId{0};
    UDPConnection udpConnection{true, config.maxUdpRate};
    std::deque<ServicePacket> servicePackets{};

    /* outgoing rays */
    std::deque<RayPacket> rayPackets{};
    std::deque<std::pair<packet_clock::time_point, RayPacket>>
        outstandingRayPackets{};
    std::map<Address, SeqNoSet> receivedAcks{};
    std::map<Address, uint64_t> sequenceNumbers{};

    /* incoming rays */
    std::map<Address, SeqNoSet> receivedPacketSeqNos{};
    std::map<Address, std::vector<std::tuple<uint64_t, bool, uint16_t>>>
        toBeAcked{};

    /* Scene Data */
    const uint8_t maxDepth{5};
    bool initialized{false};
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
    std::deque<uint64_t> finishedPathIds{};

    std::map<TreeletId, std::vector<WorkerId>> treeletToWorker{};
    std::set<TreeletId> neededTreelets{};
    std::set<TreeletId> requestedTreelets{};

    /* Always-on FD */
    FileDescriptor dummyFD{STDOUT_FILENO};

    /* Timers */
    TimerFD peerTimer{PEER_CHECK_INTERVAL};
    TimerFD workerStatsTimer{WORKER_STATS_INTERVAL};
    TimerFD workerDiagnosticsTimer{WORKER_DIAGNOSTICS_INTERVAL};
    TimerFD outQueueTimer{OUT_QUEUE_INTERVAL};
    TimerFD finishedPathsTimer{FINISHED_PATHS_INTERVAL};
    TimerFD handleRayAcknowledgementsTimer{HANDLE_ACKS_INTERVAL};

    bool terminated{false};

    ////////////////////////////////////////////////////////////////////////////
    // BENCHMARKING                                                           //
    ////////////////////////////////////////////////////////////////////////////

    using probe_clock = std::chrono::system_clock;

    struct NetStats {
        probe_clock::time_point timestamp{};

        size_t bytesSent{0};
        size_t bytesReceived{0};
        size_t packetsSent{0};
        size_t packetsReceived{0};

        void merge(const NetStats& other);
    };

    struct BenchmarkData {
        probe_clock::time_point start{};
        probe_clock::time_point end{};

        NetStats stats{};
        NetStats checkpoint{};

        std::vector<NetStats> checkpoints{};
    } benchmarkData;

    std::unique_ptr<TimerFD> benchmarkTimer{nullptr};
    std::unique_ptr<TimerFD> checkpointTimer{nullptr};
};

}  // namespace pbrt

#endif /* PBRT_CLOUD_LAMBDA_WORKER_H */
