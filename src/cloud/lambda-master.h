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
    ////////////////////////////////////////////////////////////////////////////
    // Job Information                                                        //
    ////////////////////////////////////////////////////////////////////////////

    const MasterConfiguration config;
    const TempDirectory sceneDir{"/tmp/pbrt-lambda-master"};
    const std::string jobId{uuid::generate()};

    const uint32_t numberOfLambdas;
    size_t initializedWorkers{0};

    ////////////////////////////////////////////////////////////////////////////
    // Cloud                                                                  //
    ////////////////////////////////////////////////////////////////////////////

    const std::string publicAddress;
    const std::string storageBackendUri;
    const std::unique_ptr<StorageBackend> storageBackend;
    const Address awsAddress;
    const std::string awsRegion;
    const AWSCredentials awsCredentials{};
    const std::string lambdaFunctionName{
        safe_getenv_or("PBRT_LAMBDA_FUNCTION", "pbrt-lambda-function")};

    ////////////////////////////////////////////////////////////////////////////
    // Workers                                                                //
    ////////////////////////////////////////////////////////////////////////////

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

    WorkerId currentWorkerId{1};
    std::map<WorkerId, Worker> workers{};

    ////////////////////////////////////////////////////////////////////////////
    // Scene Objects                                                          //
    ////////////////////////////////////////////////////////////////////////////

    /*** Scene Information ****************************************************/

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

    /*** Object Assignment ****************************************************/

    class ObjectManager {
      public:
        void initialize(const uint32_t numWorkers, const bool staticAssignment);
        void assignBaseObjects(Worker &worker, const int assignment);
        void assignTreelet(Worker &worker, const TreeletId treeletId);

        std::set<TreeletId> unassignedTreelets{};
        std::map<TreeletId, std::vector<WorkerId>> assignedTreelets{};

      private:
        struct SceneObjectInfo {
            SceneManager::ObjectID id;
            size_t size;
        };

        bool initialized{false};

        std::set<ObjectKey> getRecursiveDependencies(const ObjectKey &object);
        void assignObject(Worker &worker, const ObjectKey &object);

        void loadStaticAssignment(const uint32_t assignmentId,
                                  const uint32_t numWorkers);

        std::set<ObjectKey> treeletIds{};
        std::map<ObjectKey, SceneObjectInfo> sceneObjects{};
        std::map<ObjectKey, std::set<ObjectKey>> requiredDependentObjects{};
        std::map<TreeletId, std::set<ObjectKey>> treeletFlattenDependencies{};

        std::map<WorkerId, std::vector<TreeletId>> staticAssignments;
    } objectManager;

    Poller::Action::Result::Type handleMessages();
    Poller::Action::Result::Type handleWriteOutput();
    Poller::Action::Result::Type handleWriteWorkerStats();
    Poller::Action::Result::Type handleStatusMessage();
    Poller::Action::Result::Type handleJobStart();

    bool processMessage(const WorkerId workerId, const meow::Message &message);

    void aggregateQueueStats();

    std::ofstream statsOstream{};

    /* Message Queues */
    std::deque<std::pair<WorkerId, meow::Message>> incomingMessages{};

    ////////////////////////////////////////////////////////////////////////////
    // Scene Objects                                                          //
    ////////////////////////////////////////////////////////////////////////////

    size_t totalPaths{0};
    SeqNoSet finishedPathIds{};

    ////////////////////////////////////////////////////////////////////////////
    // Ray Bags                                                               //
    ////////////////////////////////////////////////////////////////////////////

    std::map<TreeletId, std::queue<RayBag>> queuedRayBags;
    std::map<TreeletId, std::queue<RayBag>> pendingRayBags;
    std::map<TreeletId, size_t> queueSize;

    ////////////////////////////////////////////////////////////////////////////
    // Timers                                                                 //
    ////////////////////////////////////////////////////////////////////////////

    const timepoint_t startTime{now()};

    timepoint_t lastActionTime{startTime};
    timepoint_t allToAllConnectStart{};
    timepoint_t generationStart{};
    timepoint_t lastFinishedRay{};

    /* Worker stats */
    WorkerStats workerStats{};
    std::chrono::seconds workerStatsInterval;

    /* Camera tile allocation */
    bool cameraRaysRemaining() const;
    Bounds2i nextCameraTile();
    void sendWorkerTile(const Worker &worker);
    size_t curTile{0};
    int tileSize;
    Point2i nTiles{};
    bool canSendTiles{false};

    ////////////////////////////////////////////////////////////////////////////
    // Other Stuff                                                            //
    ////////////////////////////////////////////////////////////////////////////

    ExecutionLoop loop{};

    FileDescriptor alwaysOnFd{STDOUT_FILENO};

    /* Timers */
    TimerFD statusPrintTimer;
    TimerFD writeOutputTimer;
    std::unique_ptr<TimerFD> exitTimer;
};

class Schedule {
  public:
  private:
};

}  // namespace pbrt

#endif /* PBRT_CLOUD_LAMBDA_MASTER_H */
