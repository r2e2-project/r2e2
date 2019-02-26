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
#include "util/timerfd.h"
#include "util/util.h"

namespace pbrt {

enum class Assignment {
    All,
    Static,
    Uniform,
};

struct MasterConfiguration {
    bool treeletStats;
    Assignment assignment;
    std::string diagnosticsDir;
    std::string workerStatsDir;
    bool sendReliably;
};

class LambdaMaster {
  public:
    LambdaMaster(const std::string &scenePath, const uint16_t listenPort,
                 const uint32_t numberOfLambdas,
                 const std::string &publicAddress,
                 const std::string &storageBackend,
                 const std::string &awsRegion,
                 const MasterConfiguration &config);

    void run();

    std::string getSummary();

  private:
    using ObjectKey = SceneManager::ObjectKey;

    struct SceneObjectInfo {
        SceneManager::ObjectID id;
        size_t size;

        /* the set of workers which have this scene object */
        std::set<uint64_t> workers;
    };

    struct Worker {
        WorkerId id;

        std::shared_ptr<TCPConnection> connection;
        Optional<Address> udpAddress{};
        Optional<Bounds2i> tile;
        std::set<ObjectKey> objects;
        size_t freeSpace{2 * 1000 * 1000 * 1000};

        WorkerStats stats;
        mutable std::ofstream statsOstream;

        struct {
            std::string logStream;
        } aws;

        Worker(const WorkerId id, std::shared_ptr<TCPConnection> &&connection)
            : id(id), connection(std::move(connection)) {}
    };

    struct WorkerRequest {
        WorkerId worker;
        TreeletId treelet;

        WorkerRequest(const WorkerId worker, const TreeletId treelet)
            : worker(worker), treelet(treelet) {}
    };

    Poller::Action::Result::Type handleMessages();
    Poller::Action::Result::Type handleWorkerRequests();
    Poller::Action::Result::Type handleWriteOutput();
    Poller::Action::Result::Type handleWriteWorkerStats();
    Poller::Action::Result::Type handleStatusMessage();

    bool processMessage(const WorkerId workerId, const meow::Message &message);
    bool processWorkerRequest(const WorkerRequest &request);
    void loadCamera();

    /* Assigning Objects */
    std::set<ObjectKey> getRecursiveDependencies(const ObjectKey &object);
    void assignObject(Worker &worker, const ObjectKey &object);
    void assignTreelet(Worker &worker, const TreeletId treeletId);

    void assignBaseSceneObjects(Worker &worker);

    // Assigns this worker a single treelet per a uniform assignment of
    // treelets over workers. Assumes that treelet ids are in [0, t], where 0
    // is the root treelet and is assigned to everyone.  Assumes that worker
    // ids are in [0, w];
    void assignTreeletsUniformly(Worker &worker);
    void assignTreelets(Worker &worker);

    void updateObjectUsage(const Worker &worker);

    void aggregateQueueStats();

    /* AWS Lambda */
    HTTPRequest generateRequest();

    const std::string lambdaFunctionName{
        safe_getenv_or("PBRT_LAMBDA_FUNCTION", "pbrt-lambda-function")};

    const std::string scenePath;
    const uint32_t numberOfLambdas;
    const std::string publicAddress;
    const std::string storageBackendUri;
    const std::unique_ptr<StorageBackend> storageBackend;
    const Address awsAddress;
    const std::string awsRegion;
    const AWSCredentials awsCredentials{};

    ExecutionLoop loop{};
    std::shared_ptr<UDPConnection> udpConnection{};

    WorkerId currentWorkerID{1};
    std::map<WorkerId, Worker> workers{};

    /* Message Queues */
    std::deque<std::pair<WorkerId, meow::Message>> incomingMessages;

    /* Worker Requests */
    std::deque<WorkerRequest> pendingWorkerRequests;

    /* Scene Data */
    int tileSize{32};
    std::vector<std::unique_ptr<Transform>> transformCache{};
    std::shared_ptr<Camera> camera{};
    std::unique_ptr<FilmTile> filmTile{};
    size_t totalPaths{0};

    /* Scene Objects */
    Bounds2i sampleBounds;
    std::vector<uint32_t> tiles;
    std::map<ObjectKey, SceneObjectInfo> sceneObjects;

    std::set<ObjectKey> treeletIds;
    std::stack<ObjectKey> unassignedTreelets;
    std::vector<std::tuple<uint64_t, uint64_t>> treeletPriority;

    std::map<ObjectKey, std::set<ObjectKey>> requiredDependentObjects;
    std::map<TreeletId, std::set<ObjectKey>> treeletFlattenDependencies;
    std::map<TreeletId, size_t> treeletTotalSizes;

    /* Always-on FD */
    FileDescriptor dummyFD{STDOUT_FILENO};

    /* Timers */
    TimerFD workerRequestTimer;
    TimerFD statusPrintTimer;
    TimerFD writeOutputTimer;
    TimerFD writeWorkerStatsTimer;

    const std::chrono::steady_clock::time_point startTime{
        std::chrono::steady_clock::now()};

    /* Worker stats */
    WorkerStats workerStats;
    DemandTracker demandTracker{};
    size_t initializedWorkers{0};
    size_t diagnosticsReceived{0};

    /* Static Assignments */
    void loadStaticAssignment(const uint32_t numWorkers);
    std::map<WorkerId, std::vector<TreeletId>> staticAssignments;

    const MasterConfiguration config;
};

class Schedule {
  public:
  private:
};

}  // namespace pbrt

#endif /* PBRT_CLOUD_LAMBDA_MASTER_H */
