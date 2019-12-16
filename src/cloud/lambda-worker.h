#ifndef PBRT_CLOUD_LAMBDA_WORKER_H
#define PBRT_CLOUD_LAMBDA_WORKER_H

#include <cstring>
#include <deque>
#include <fstream>
#include <future>
#include <iostream>
#include <random>
#include <string>
#include <thread>
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
#include "net/s3.h"
#include "storage/backend.h"
#include "util/seq_no_set.h"
#include "util/temp_dir.h"
#include "util/timerfd.h"
#include "util/units.h"

#define TLOG(tag) LOG(INFO) << "[" #tag "] "

namespace pbrt {

constexpr std::chrono::milliseconds WORKER_DIAGNOSTICS_INTERVAL{2'000};
constexpr std::chrono::milliseconds FINISHED_PATHS_INTERVAL{2'500};
constexpr std::chrono::milliseconds LEASE_LOG_INTERVAL{5'000};
constexpr std::chrono::milliseconds WORKER_STATS_INTERVAL{1'000};

constexpr size_t MAX_BAG_SIZE{4 * 1024 * 1024}; // 4 MiB

struct WorkerConfiguration {
    bool sendReliably;
    uint64_t maxUdpRate;
    int samplesPerPixel;
    FinishedRayAction finishedRayAction;
    float rayActionsLogRate;
    float packetsLogRate;
    bool collectDiagnostics;
    bool logLeases;
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
    using steady_clock = std::chrono::steady_clock;
    using rays_clock = std::chrono::system_clock;

    enum class RayAction {
        Generated,
        Traced,
        Pending,
        Queued,
        Sent,
        Received,
        Finished
    };

    ////////////////////////////////////////////////////////////////////////////
    // MEMBER FUNCTIONS                                                       //
    ////////////////////////////////////////////////////////////////////////////

    void processMessage(const meow::Message& message);

    void initializeScene();
    void loadCamera();
    void loadSampler();
    void loadLights();
    void loadFakeScene();

    /* rays.cpp */
    Poller::Action::Result::Type handleTraceQueue();
    Poller::Action::Result::Type handleOutQueue();
    Poller::Action::Result::Type handleFinishedQueue();
    Poller::Action::Result::Type handleFinishedPaths();

    /* messages.cpp */
    Poller::Action::Result::Type handleMessages();

    /* logs.cpp */
    Poller::Action::Result::Type handleWorkerStats();
    Poller::Action::Result::Type handleDiagnostics();

    void generateRays(const Bounds2i& cropWindow);
    void getObjects(const protobuf::GetObjects& objects);

    void pushTraceQueue(RayStatePtr&& state);
    RayStatePtr popTraceQueue();

    void logRayAction(const RayState& state, const RayAction action,
                      const WorkerId otherParty = -1);

    ////////////////////////////////////////////////////////////////////////////
    // MEMBER VARIABLES                                                       //
    ////////////////////////////////////////////////////////////////////////////

    const WorkerConfiguration config;

    /* Logging & Diagnostics */
    const std::string logBase{"pbrt-worker"};
    const std::string infoLogName{logBase + ".INFO"};
    std::string logPrefix{"logs/"};
    const bool trackRays{config.rayActionsLogRate > 0};

    std::bernoulli_distribution coin{0.5};
    std::mt19937 randEngine{std::random_device{}()};

    WorkerStats workerStats;
    WorkerDiagnostics lastDiagnostics;

    const Address coordinatorAddr;
    const UniqueDirectory workingDirectory;
    ExecutionLoop loop{};
    std::unique_ptr<StorageBackend> storageBackend;
    std::shared_ptr<TCPConnection> coordinatorConnection;
    meow::MessageParser messageParser{};
    Optional<WorkerId> workerId;
    Optional<std::string> jobId;
    std::string outputName;
    bool terminated{false};

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

    /* Rays */
    std::deque<RayStatePtr> traceQueue{};
    std::deque<FinishedRay> finishedQueue{};
    std::map<TreeletId, std::deque<RayStatePtr>> outQueue{};
    std::map<TreeletId, size_t> outQueueBytes{};
    size_t outQueueSize{0};
    std::deque<uint64_t> finishedPathIds{};

    std::map<TreeletId, std::queue<std::pair<size_t, std::string>>> sendQueue{};

    /* Always-on FD */
    FileDescriptor dummyFD{STDOUT_FILENO};

    /* Timers */
    TimerFD workerStatsTimer{WORKER_STATS_INTERVAL};
    TimerFD workerDiagnosticsTimer{WORKER_DIAGNOSTICS_INTERVAL};
    TimerFD finishedPathsTimer{FINISHED_PATHS_INTERVAL};

    const steady_clock::time_point workStart{steady_clock::now()};

    ////////////////////////////////////////////////////////////////////////////
    // Transfer Agent                                                         //
    ////////////////////////////////////////////////////////////////////////////

    class TransferAgent {
      public:
        struct Action {
            enum Type { Download, Upload };

            uint64_t id;
            Type type;
            std::string key;
            std::string data;

            Action(const uint64_t id, const Type type, const std::string& key,
                   std::string&& data)
                : id(id), type(type), key(key), data(move(data)) {}
        };

      private:
        uint64_t nextId{1};

        struct S3Config {
            AWSCredentials credentials{};
            std::string region{};
            std::string bucket{};
            std::string prefix{};

            std::string endpoint{};
            Address address{};
        } clientConfig;

        std::queue<Action> results{};

        std::mutex mtx;
        std::atomic<bool> isEmpty{true};
        std::map<uint64_t, std::future<void>> runningTasks;

        void doAction(Action&& action);

      public:
        TransferAgent(const S3StorageBackend& backend);
        uint64_t requestDownload(const std::string& key);
        uint64_t requestUpload(const std::string& key, std::string&& data);

        bool empty();
        Action pop();
    };
};

}  // namespace pbrt

#endif /* PBRT_CLOUD_LAMBDA_WORKER_H */
