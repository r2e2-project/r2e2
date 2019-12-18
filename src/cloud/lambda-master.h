#ifndef PBRT_CLOUD_LAMBDA_MASTER_H
#define PBRT_CLOUD_LAMBDA_MASTER_H

#include <fstream>
#include <map>
#include <memory>
#include <set>
#include <stack>
#include <string>
#include <vector>

#include "cloud/estimators.h"
#include "cloud/lambda.h"
#include "cloud/manager.h"
#include "cloud/stats.h"
#include "core/camera.h"
#include "core/geometry.h"
#include "core/transform.h"
#include "execution/connection.h"
#include "execution/loop.h"
#include "execution/meow/message.h"
#include "net/address.h"
#include "net/aws.h"
#include "net/http_request.h"
#include "storage/backend.h"
#include "util/optional.h"
#include "util/seq_no_set.h"
#include "util/temp_dir.h"
#include "util/timerfd.h"
#include "util/util.h"
#include "util/uuid.h"

namespace pbrt {

struct Assignment {
    // clang-format off
    static constexpr int All        = (1 << 0);
    static constexpr int Static     = (1 << 1);
    static constexpr int Uniform    = (1 << 2);
    static constexpr int Debug      = (1 << 3); /* only assigns T0 to one worker */
    // clang-format on
};

enum class FinishedRayAction { Discard, SendBack, Upload };

enum class Task {
    RayTracing,
    NetworkTest,
};

struct MasterConfiguration {
    Task task;
    int assignment; /* look at `struct Assignment` */
    std::string assignmentFile;
    FinishedRayAction finishedRayAction;
    bool sendReliably;
    uint64_t maxUdpRate;
    int samplesPerPixel;
    bool collectDebugLogs;
    bool collectDiagnostics;
    bool logLeases;
    uint64_t workerStatsInterval;
    float rayActionsLogRate;
    float packetsLogRate;
    std::string logsDirectory;
    Optional<Bounds2i> cropWindow;
    int tileSize;
    std::chrono::seconds timeout;
    std::string jobSummaryPath;
    uint64_t newTileThreshold;
};

class LambdaMaster {
  public:
    LambdaMaster(const uint16_t listenPort, const uint32_t numberOfLambdas,
                 const std::string &publicAddress,
                 const std::string &storageBackend,
                 const std::string &awsRegion,
                 const MasterConfiguration &config);

    ~LambdaMaster();

    void run();

    void printJobSummary() const;
    void dumpJobSummary() const;

  private:
    const MasterConfiguration config;

    ////////////////////////////////////////////////////////////////////////////
    // Scene Objects                                                          //
    ////////////////////////////////////////////////////////////////////////////

    struct SceneData {
      public:
        Bounds2i sampleBounds{};
        Vector2i sampleExtent{};
        std::shared_ptr<Camera> camera{};
        std::shared_ptr<Sampler> sampler{};
        std::vector<std::unique_ptr<Transform>> transformCache{};

        void initialize(const int samplesPerPixel,
                        const Optional<Bounds2i> &cropWindow);

      private:
        bool initialized{false};
        void loadCamera(const Optional<Bounds2i> &cropWindow);
        void loadSampler(const int samplesPerPixel);
    } scene;

    struct SceneObjectInfo {
        SceneManager::ObjectID id;
        size_t size;
    };

    struct Worker {
        WorkerId id;
        bool initialized{false};
        std::shared_ptr<TCPConnection> connection;
        std::set<ObjectKey> objects{};
        WorkerStats stats{};
        uint64_t nextStatusLogTimestamp{0};
        std::string awsLogStream{};

        Worker(const WorkerId id, std::shared_ptr<TCPConnection> &&connection)
            : id(id), connection(std::move(connection)) {}
    };

    Poller::Action::Result::Type handleMessages();
    Poller::Action::Result::Type handleWriteOutput();
    Poller::Action::Result::Type handleWriteWorkerStats();
    Poller::Action::Result::Type handleStatusMessage();
    Poller::Action::Result::Type handleJobStart();

    bool processMessage(const WorkerId workerId, const meow::Message &message);

    /* Assigning Objects */
    std::set<ObjectKey> getRecursiveDependencies(const ObjectKey &object);
    void assignObject(Worker &worker, const ObjectKey &object);
    void assignTreelet(Worker &worker, const TreeletId treeletId);

    void assignBaseSceneObjects(Worker &worker);
    void updateObjectUsage(const Worker &worker);
    void aggregateQueueStats();

    /* AWS Lambda */
    HTTPRequest generateRequest();

    const std::string lambdaFunctionName{
        safe_getenv_or("PBRT_LAMBDA_FUNCTION", "pbrt-lambda-function")};

    const TempDirectory sceneDir{"/tmp/pbrt-lambda-master"};
    const uint32_t numberOfLambdas;
    const std::string publicAddress;
    const std::string storageBackendUri;
    const std::unique_ptr<StorageBackend> storageBackend;
    const Address awsAddress;
    const std::string awsRegion;
    const AWSCredentials awsCredentials{};
    std::ofstream statsOstream{};

    ExecutionLoop loop{};

    const std::string jobId{uuid::generate()};
    WorkerId currentWorkerId{1};
    std::map<WorkerId, Worker> workers{};

    /* Message Queues */
    std::deque<std::pair<WorkerId, meow::Message>> incomingMessages{};

    ////////////////////////////////////////////////////////////////////////////
    // Scene Objects                                                          //
    ////////////////////////////////////////////////////////////////////////////

    size_t totalPaths{0};
    SeqNoSet finishedPathIds{};

    std::vector<uint32_t> tiles;
    std::map<ObjectKey, SceneObjectInfo> sceneObjects;
    std::set<ObjectKey> treeletIds;
    std::map<ObjectKey, std::set<ObjectKey>> requiredDependentObjects;
    std::map<TreeletId, std::set<ObjectKey>> treeletFlattenDependencies;
    std::map<TreeletId, size_t> treeletTotalSizes;

    std::set<TreeletId> unassignedTreelets;
    std::map<TreeletId, std::vector<WorkerId>> assignedTreelets;

    ////////////////////////////////////////////////////////////////////////////
    // Ray Bags                                                               //
    ////////////////////////////////////////////////////////////////////////////

    std::map<TreeletId, std::queue<RayBag>> queuedRayBags;
    std::map<TreeletId, std::queue<RayBag>> pendingRayBags;
    std::map<TreeletId, size_t> queueSize;

    ////////////////////////////////////////////////////////////////////////////
    // Timers                                                                 //
    ////////////////////////////////////////////////////////////////////////////

    FileDescriptor alwaysOnFd{STDOUT_FILENO};
    TimerFD statusPrintTimer;
    TimerFD writeOutputTimer;
    std::unique_ptr<TimerFD> exitTimer;

    const timepoint_t startTime{now()};

    timepoint_t lastActionTime{startTime};
    timepoint_t allToAllConnectStart{};
    timepoint_t generationStart{};
    timepoint_t lastFinishedRay{};

    /* Worker stats */
    WorkerStats workerStats{};
    std::chrono::seconds workerStatsInterval;
    size_t initializedWorkers{0};

    /* Static Assignments */
    void loadStaticAssignment(const uint32_t assignmentId,
                              const uint32_t numWorkers);

    std::map<WorkerId, std::vector<TreeletId>> staticAssignments;

    /* Camera tile allocation */
    bool cameraRaysRemaining() const;
    Bounds2i nextCameraTile();
    void sendWorkerTile(const Worker &worker);
    size_t curTile{0};
    int tileSize;
    Point2i nTiles{};
    bool canSendTiles{false};
};

class Schedule {
  public:
  private:
};

}  // namespace pbrt

#endif /* PBRT_CLOUD_LAMBDA_MASTER_H */
