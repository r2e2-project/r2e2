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

constexpr std::chrono::milliseconds STATUS_PRINT_INTERVAL{1'000};
constexpr std::chrono::milliseconds WRITE_OUTPUT_INTERVAL{10'000};

struct Assignment {
    // clang-format off
    static constexpr int None    = 0;
    static constexpr int All     = (1 << 0);
    static constexpr int Static  = (1 << 1);
    static constexpr int Uniform = (1 << 2);
    static constexpr int Debug   = (1 << 3); /* only assigns T0 to one worker */
    // clang-format on
};

enum class FinishedRayAction { Discard, SendBack, Upload };

struct MasterConfiguration {
    int assignment; /* look at `struct Assignment` */
    std::string assignmentFile;
    FinishedRayAction finishedRayAction;
    bool sendReliably;
    uint64_t maxUdpRate;
    int samplesPerPixel;
    bool collectDebugLogs;
    bool collectDiagnostics;
    bool logLeases;
    uint64_t workerStatsWriteInterval;
    float rayActionsLogRate;
    float packetsLogRate;
    std::string logsDirectory;
    Optional<Bounds2i> cropWindow;
    int tileSize;
    std::chrono::seconds timeout;
    std::string jobSummaryPath;
    uint64_t newTileThreshold;

    std::vector<std::pair<std::string, uint32_t>> engines;
};

class LambdaMaster {
  public:
    LambdaMaster(const uint16_t listenPort, const uint32_t numberOfWorkers,
                 const std::string &publicAddress,
                 const std::string &storageBackend,
                 const std::string &awsRegion,
                 const MasterConfiguration &config);

    ~LambdaMaster();

    void run();

    protobuf::JobSummary getJobSummary() const;
    void printJobSummary() const;
    void dumpJobSummary() const;

  private:
    using steady_clock = std::chrono::steady_clock;

    ////////////////////////////////////////////////////////////////////////////
    // Job Information                                                        //
    ////////////////////////////////////////////////////////////////////////////

    const MasterConfiguration config;
    const TempDirectory sceneDir{"/tmp/pbrt-lambda-master"};
    const std::string jobId{uuid::generate()};

    const uint32_t numberOfWorkers;
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
        steady_clock::time_point lastSeen{};
        std::string awsLogStream{};

        std::set<TreeletId> treelets{};
        std::set<ObjectKey> objects{};

        std::set<RayBagInfo> assignedRayBags{};

        // Statistics
        WorkerStats stats{};
        std::pair<bool, WorkerStats> lastStats{true, {}};

        Worker(const WorkerId id, std::shared_ptr<TCPConnection> &&connection)
            : id(id), connection(std::move(connection)) {}
    };

    WorkerId currentWorkerId{1};
    std::vector<Worker> workers{};

    ////////////////////////////////////////////////////////////////////////////
    // Treelets                                                               //
    ////////////////////////////////////////////////////////////////////////////

    struct Treelet {
        TreeletId id;

        std::set<WorkerId> workers;

        // Statistics
        TreeletStats stats;
        std::pair<bool, TreeletStats> lastStats{true, {}};

        Treelet(const TreeletId id) : id(id) {}
    };

    std::vector<Treelet> treelets{};

    ////////////////////////////////////////////////////////////////////////////
    // Treelet <-> Worker Assignments                                         //
    ////////////////////////////////////////////////////////////////////////////

    // TODO

    ////////////////////////////////////////////////////////////////////////////
    // Communication                                                          //
    ////////////////////////////////////////////////////////////////////////////

    /*** Messages *************************************************************/

    /* processes incoming messages; called by handleMessages */
    void processMessage(const WorkerId workerId, const meow::Message &message);

    /* process incoming messages */
    Poller::Action::Result::Type handleMessages();

    /* tell the workers to fetch base objects and generate rays */
    Poller::Action::Result::Type handleJobStart();

    /* a queue for incoming messages */
    std::deque<std::pair<WorkerId, meow::Message>> incomingMessages{};

    /*** Ray Bags *************************************************************/

    Poller::Action::Result::Type handleQueuedRayBags();

    /* ray bags that are going to be assigned to workers */
    std::map<TreeletId, std::queue<RayBagInfo>> queuedRayBags;

    /* ray bags that there are no workers for them */
    std::map<TreeletId, std::queue<RayBagInfo>> pendingRayBags;

    void moveFromPendingToQueued(const TreeletId treeletId);
    void moveFromQueuedToPending(const TreeletId treeletId);

    std::map<TreeletId, size_t> queueSize;

    ////////////////////////////////////////////////////////////////////////////
    // Stats                                                                  //
    ////////////////////////////////////////////////////////////////////////////

    WorkerStats aggregatedStats{};

    /*** Outputting stats *****************************************************/

    void recordEnqueue(const WorkerId workerId, const RayBagInfo &info);
    void recordAssign(const WorkerId workerId, const RayBagInfo &info);
    void recordDequeue(const WorkerId workerId, const RayBagInfo &info);

    /* object for writing worker & treelet stats */
    std::ofstream wsStream{};
    std::ofstream tlStream{};

    /* write worker stats periodically */
    Poller::Action::Result::Type handleWorkerStats();

    /* prints the status message every second */
    Poller::Action::Result::Type handleStatusMessage();

    /*** Timepoints ***********************************************************/

    const steady_clock::time_point startTime{steady_clock::now()};

    steady_clock::time_point lastActionTime{startTime};
    steady_clock::time_point generationStart{};
    steady_clock::time_point lastFinishedRay{};

    ////////////////////////////////////////////////////////////////////////////
    // Scene Objects                                                          //
    ////////////////////////////////////////////////////////////////////////////

    /*** Scene Information ****************************************************/

    struct SceneData {
      public:
        size_t totalPaths{0};
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
    } scene{};

    /*** Object Assignment ****************************************************/

    class ObjectManager {
      public:
        void initialize(const uint32_t numWorkers, const bool staticAssignment);
        void assignBaseObjects(Worker &worker, std::vector<Treelet> &treelets,
                               const int assignment);
        void assignTreelet(Worker &worker, Treelet &treelet);

        std::set<TreeletId> unassignedTreelets{};

      private:
        bool initialized{false};

        void assignObject(Worker &worker, const ObjectKey &object);
        void loadStaticAssignment(const uint32_t assignmentId,
                                  const uint32_t numWorkers);

        std::set<ObjectKey> treeletIds{};
        std::map<WorkerId, std::vector<TreeletId>> staticAssignments{};
    } objectManager{};

    /*** Tiles ****************************************************************/

    class Tiles {
      public:
        Bounds2i nextCameraTile();
        bool cameraRaysRemaining() const;
        void sendWorkerTile(const Worker &worker);

        Tiles() = default;
        Tiles(const int tileSize, const Bounds2i &bounds, const long int spp,
              const uint32_t numWorkers);

        int tileSize{0};
        bool canSendTiles{false};

      private:
        Bounds2i sampleBounds{};
        Point2i nTiles{};
        size_t curTile{0};
    } tiles{};

    /*** Accumulation *********************************************************/

    Poller::Action::Result::Type handleWriteOutput();

    ////////////////////////////////////////////////////////////////////////////
    // Other Stuff                                                            //
    ////////////////////////////////////////////////////////////////////////////

    ExecutionLoop loop{};

    FileDescriptor alwaysOnFd{STDOUT_FILENO};

    /* Timers */
    TimerFD statusPrintTimer{STATUS_PRINT_INTERVAL};
    TimerFD writeOutputTimer{WRITE_OUTPUT_INTERVAL};
    TimerFD workerStatsWriteTimer;

    std::unique_ptr<TimerFD> exitTimer;
};

}  // namespace pbrt

#endif /* PBRT_CLOUD_LAMBDA_MASTER_H */
