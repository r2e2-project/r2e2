#ifndef PBRT_CLOUD_LAMBDA_MASTER_H
#define PBRT_CLOUD_LAMBDA_MASTER_H

#include <pbrt/core/geometry.h>
#include <pbrt/main.h>

#include <fstream>
#include <map>
#include <memory>
#include <random>
#include <set>
#include <stack>
#include <string>
#include <vector>

#include "common/lambda.h"
#include "common/stats.h"
#include "execution/connection.h"
#include "execution/loop.h"
#include "execution/meow/message.h"
#include "net/address.h"
#include "net/aws.h"
#include "net/http_request.h"
#include "net/ws_server.h"
#include "r2t2.pb.h"
#include "schedulers/scheduler.h"
#include "storage/backend.h"
#include "util/optional.h"
#include "util/seq_no_set.h"
#include "util/temp_dir.h"
#include "util/timelog.h"
#include "util/timerfd.h"
#include "util/util.h"
#include "util/uuid.h"

namespace r2t2 {

constexpr std::chrono::milliseconds STATUS_PRINT_INTERVAL{1'000};
constexpr std::chrono::milliseconds RESCHEDULE_INTERVAL{1'000};
constexpr std::chrono::milliseconds WORKER_INVOCATION_INTERVAL{2'000};
constexpr std::chrono::milliseconds SUBSCRIBERS_INTERVAL{1'000};

struct MasterConfiguration {
    int samplesPerPixel;
    int maxPathDepth;
    bool collectDebugLogs;
    uint64_t workerStatsWriteInterval;
    float rayLogRate;
    float bagLogRate;
    std::string logsDirectory;
    Optional<pbrt::Bounds2i> cropWindow;
    int tileSize;
    std::chrono::seconds timeout;
    std::string jobSummaryPath;
    uint64_t newTileThreshold;

    std::vector<std::string> memcachedServers;
    std::vector<std::pair<std::string, uint32_t>> engines;
};

class LambdaMaster {
  public:
    LambdaMaster(const uint16_t listenPort, const uint16_t clientPort,
                 const uint32_t maxWorkers, const uint32_t rayGenerators,
                 const std::string &publicAddress,
                 const std::string &storageBackend,
                 const std::string &awsRegion,
                 std::unique_ptr<Scheduler> &&scheduler,
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
    const std::string jobId;

    ////////////////////////////////////////////////////////////////////////////
    // Job Info Server                                                        //
    ////////////////////////////////////////////////////////////////////////////

    Poller::Action::Result::Type handleSubscribers();

    std::unique_ptr<WebSocketTCPServer> wsServer{nullptr};
    std::map<uint64_t, size_t> subscribers{};
    std::string samplesUrlPrefix{};

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
        enum class State { Active, FinishingUp, Terminating, Terminated };
        enum class Role { Generator, Tracer, Aggregator };

        WorkerId id;
        State state{State::Active};
        Role role;
        std::shared_ptr<TCPConnection> connection;
        steady_clock::time_point lastSeen{};
        std::string awsLogStream{};

        std::vector<TreeletId> treelets{};
        std::set<pbrt::ObjectKey> objects{};

        std::set<RayBagInfo> outstandingRayBags{};
        size_t outstandingBytes{0};

        struct {
            uint64_t camera{0};
            uint64_t generated{0};
            uint64_t dequeued{0};

            uint64_t terminated{0};
            uint64_t enqueued{0};
        } rays;

        uint64_t activeRays() const {
            return rays.camera + rays.generated + rays.dequeued -
                   rays.terminated - rays.enqueued;
        }

        // Statistics
        bool isLogged{true};
        WorkerStats stats{};
        WorkerStats lastStats;

        Worker(const WorkerId id, const Role role,
               std::shared_ptr<TCPConnection> &&connection)
            : id(id), role(role), connection(std::move(connection)) {
            Worker::activeCount[role]++;
        }

        std::string toString() const;

        static std::map<Role, size_t> activeCount;
        static WorkerId nextId;
    };

    std::vector<Worker> workers{};
    const uint32_t maxWorkers;
    const uint32_t rayGenerators;
    uint32_t finishedRayGenerators{0};
    uint32_t initializedWorkers{0};

    std::deque<WorkerId> freeWorkers{};

    ////////////////////////////////////////////////////////////////////////////
    // Treelets                                                               //
    ////////////////////////////////////////////////////////////////////////////

    struct Treelet {
        TreeletId id;
        size_t pendingWorkers{0};
        std::set<WorkerId> workers{};
        std::pair<bool, TreeletStats> lastStats{true, {}};

        Treelet(const TreeletId id) : id(id) {}
    };

    std::vector<Treelet> treelets{};
    std::vector<TreeletStats> treeletStats{};

    ////////////////////////////////////////////////////////////////////////////
    // Scheduler                                                              //
    ////////////////////////////////////////////////////////////////////////////

    /* this function is periodically called; it calls the scheduler,
       and if a new schedule is available, it executes it */
    Poller::Action::Result::Type handleReschedule();

    Poller::Action::Result::Type handleWorkerInvocation();

    void executeSchedule(const Schedule &schedule);

    /* requests invoking n workers */
    void invokeWorkers(const size_t n);

    std::unique_ptr<Scheduler> scheduler;
    std::deque<TreeletId> treeletsToSpawn;
    std::string invocationPayload;

    ////////////////////////////////////////////////////////////////////////////
    // Worker <-> Object Assignments                                          //
    ////////////////////////////////////////////////////////////////////////////

    void assignObject(Worker &worker, const pbrt::ObjectKey &object);
    void assignBaseObjects(Worker &worker);
    void assignTreelet(Worker &worker, Treelet &treelet);

    std::set<TreeletId> unassignedTreelets{};

    ////////////////////////////////////////////////////////////////////////////
    // Communication                                                          //
    ////////////////////////////////////////////////////////////////////////////

    /*** Messages *************************************************************/

    /* processes incoming messages; called by handleMessages */
    void processMessage(const WorkerId workerId, const meow::Message &message);

    /* process incoming messages */
    Poller::Action::Result::Type handleMessages();

    /* a queue for incoming messages */
    std::deque<std::pair<WorkerId, meow::Message>> incomingMessages{};

    /*** Ray Bags *************************************************************/

    bool assignWork(Worker &worker);

    Poller::Action::Result::Type handleQueuedRayBags();

    /* ray bags that are going to be assigned to workers */
    std::vector<std::queue<RayBagInfo>> queuedRayBags;
    size_t queuedRayBagsCount{0};

    /* ray bags that there are no workers for them */
    std::vector<std::queue<RayBagInfo>> pendingRayBags;

    /* sample bags */
    std::vector<RayBagInfo> sampleBags;

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
    steady_clock::time_point lastGeneratorDone{startTime};
    steady_clock::time_point lastFinishedRay{};
    steady_clock::time_point lastActionTime{startTime};

    ////////////////////////////////////////////////////////////////////////////
    // Scene Objects                                                          //
    ////////////////////////////////////////////////////////////////////////////

    /*** Scene Information ****************************************************/

    struct SceneData {
      public:
        pbrt::scene::Base base{};

        pbrt::Bounds2i sampleBounds{};
        pbrt::Vector2i sampleExtent{};
        size_t totalPaths{0};

        SceneData() {}
        SceneData(const std::string &scenePath, const int samplesPerPixel,
                  const Optional<pbrt::Bounds2i> &cropWindow);
    } scene{};

    /*** Tiles ****************************************************************/

    class Tiles {
      public:
        pbrt::Bounds2i nextCameraTile();
        bool cameraRaysRemaining() const;
        void sendWorkerTile(Worker &worker);

        Tiles() = default;
        Tiles(const int tileSize, const pbrt::Bounds2i &bounds,
              const long int spp, const uint32_t numWorkers);

        int tileSize{0};

      private:
        pbrt::Bounds2i sampleBounds{};
        pbrt::Point2i nTiles{};
        size_t curTile{0};
        size_t tileSpp{};
    } tiles{};

    ////////////////////////////////////////////////////////////////////////////
    // Other Stuff                                                            //
    ////////////////////////////////////////////////////////////////////////////

    ExecutionLoop loop{};

    FileDescriptor alwaysOnFd{STDOUT_FILENO};

    /* Timers */
    TimerFD statusPrintTimer{STATUS_PRINT_INTERVAL};
    TimerFD workerInvocationTimer{WORKER_INVOCATION_INTERVAL};
    TimerFD rescheduleTimer{RESCHEDULE_INTERVAL,
                            std::chrono::milliseconds{500}};
    TimerFD workerStatsWriteTimer;
    TimerFD serviceSubscribersTimer{SUBSCRIBERS_INTERVAL};

    std::unique_ptr<TimerFD> jobExitTimer;
    std::unique_ptr<TimerFD> jobTimeoutTimer;

    std::mt19937 randEngine{std::random_device{}()};
};

}  // namespace r2t2

#endif /* PBRT_CLOUD_LAMBDA_MASTER_H */
