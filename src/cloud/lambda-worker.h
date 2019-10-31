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
#include "util/units.h"

namespace pbrt {

constexpr std::chrono::milliseconds PEER_CHECK_INTERVAL{250};
constexpr std::chrono::milliseconds HANDLE_ACKS_INTERVAL{10};
constexpr std::chrono::milliseconds WORKER_STATS_INTERVAL{1'000};
constexpr std::chrono::milliseconds WORKER_DIAGNOSTICS_INTERVAL{2'000};
constexpr std::chrono::milliseconds KEEP_ALIVE_INTERVAL{40'000};
constexpr std::chrono::milliseconds FINISHED_PATHS_INTERVAL{2'500};
constexpr std::chrono::milliseconds PACKET_TIMEOUT{20};
constexpr std::chrono::milliseconds INACTIVITY_THRESHOLD{1'00};
// constexpr std::chrono::milliseconds TREELET_PEER_TIMEOUT{200};
constexpr std::chrono::milliseconds RECONNECTS_INTERVAL{2'000};

constexpr uint64_t DEFAULT_SEND_RATE{1'400 * 8};

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

        struct Diagnostics {
            uint64_t bytesSent{0};
            uint64_t bytesReceived{0};
        };

        WorkerId id;
        Address address;
        State state{State::Connecting};
        packet_clock::time_point nextKeepAlive{};
        int32_t seed{0};
        uint32_t tries{0};

        Pacer pacer{true, DEFAULT_SEND_RATE};
        packet_clock::time_point lastReceivedAck{packet_clock::now()};

        std::set<TreeletId> treelets{};

        Diagnostics diagnostics;

        Worker(const WorkerId id, Address&& addr)
            : id(id), address(std::move(addr)) {}

        void reset() {
            state = State::Connecting;
            seed = 0;
            tries = 0;
            pacer = {true, DEFAULT_SEND_RATE};
        }
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
                      const uint64_t ackId = 0, const bool tracked = false);

        ServicePacket(const ServicePacket&) = delete;
        ServicePacket(ServicePacket&&) = default;
        ServicePacket& operator=(const ServicePacket&) = delete;
        ServicePacket& operator=(ServicePacket&&) = default;
    };

    struct RayPacket {
        Address destination;
        WorkerId destinationId;
        TreeletId targetTreelet;

        bool retransmission{false};
        bool reliable{false};
        bool tracked{false};
        uint64_t sequenceNumber;
        size_t attempt{0};
        size_t length{meow::Message::HEADER_LENGTH + sizeof(uint32_t)};

        char header[meow::Message::HEADER_LENGTH];
        uint32_t queueLength;
        std::deque<std::unique_ptr<RayState>> rays;

        packet_clock::time_point sentAt;

        RayPacket(const Address& addr, const WorkerId destId,
                  const TreeletId targetTreelet, const uint32_t queueLength,
                  const bool reliable = false,
                  const uint64_t sequenceNumber = 0,
                  const bool tracked = false);

        RayPacket(const RayPacket&) = delete;
        RayPacket(RayPacket&&) = default;
        RayPacket& operator=(const RayPacket&) = delete;
        RayPacket& operator=(RayPacket&&) = default;

        void addRay(RayStatePtr&& ray);
        void incrementAttempts();
        size_t raysLength() const;
        struct iovec* iov();
        size_t iovCount() const { return iovCount_; }

      private:
        struct iovec iov_[20] = {
            {.iov_base = nullptr, .iov_len = 25},
            {.iov_base = nullptr, .iov_len = sizeof(uint32_t)}};

        size_t iovCount_{2};
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

    ////////////////////////////////////////////////////////////////////////////
    // MEMBER FUNCTIONS                                                       //
    ////////////////////////////////////////////////////////////////////////////

    bool processMessage(const meow::Message& message);
    void initializeScene();

    void loadCamera();
    void loadSampler();
    void loadLights();
    void loadFakeScene();

    Poller::Action::Result::Type handleRayQueue();
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

    Poller::Action::Result::Type handleReconnects();

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
    std::bernoulli_distribution coin{0.5};

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

    std::set<WorkerId> reconnectRequests;

    struct Lease {
        packet_clock::time_point expiresAt{packet_clock::now() +
                                           INACTIVITY_THRESHOLD};

        WorkerId workerId{0};
        bool small{false};
        uint32_t allocation{DEFAULT_SEND_RATE};
        uint32_t queueSize{1'400};
    };

    const packet_clock::time_point workStart{packet_clock::now()};
    std::map<Address, Lease> activeLeases{};  // used by the receiver

    std::map<TreeletId, std::pair<WorkerId, packet_clock::time_point>>
        workerForTreelet;  // used by the sender

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
    std::set<Address> toBeAcked{};

    /* Scene Data */
    const uint8_t maxDepth{5};
    bool initialized{false};
    std::vector<std::unique_ptr<Transform>> transformCache{};
    std::shared_ptr<Camera> camera{};
    std::unique_ptr<FilmTile> filmTile{};
    std::shared_ptr<GlobalSampler> sampler{};
    Vector2i sampleExtent{};
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
    std::map<TreeletId, size_t> outQueueLengthBytes{};
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
    TimerFD finishedPathsTimer{FINISHED_PATHS_INTERVAL};
    TimerFD handleRayAcknowledgementsTimer{HANDLE_ACKS_INTERVAL};
    TimerFD reconnectTimer{RECONNECTS_INTERVAL};

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
