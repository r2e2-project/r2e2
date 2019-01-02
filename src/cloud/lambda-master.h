#ifndef PBRT_CLOUD_LAMBDA_MASTER_H
#define PBRT_CLOUD_LAMBDA_MASTER_H

#include <map>
#include <memory>
#include <set>
#include <stack>
#include <string>
#include <vector>

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
#include "util/optional.h"
#include "util/timerfd.h"

namespace pbrt {

class LambdaMaster {
  public:
    LambdaMaster(const std::string &scenePath, const uint16_t listenPort,
                 const uint32_t numberOfLambdas,
                 const std::string &publicAddress,
                 const std::string &storageBackend,
                 const std::string &awsRegion);

    void run();

    static constexpr int TILE_SIZE = 32;

  private:
    using ObjectTypeID = SceneManager::ObjectTypeID;

    struct SceneObjectInfo {
        SceneManager::ObjectID id;
        size_t size;
        std::set<uint64_t>
            workers; /* the set of workers which have this scene object */
        float rayRequestsPerSecond{
            1}; /* the production rate for rays that need this scene object */
        float raysProcessedPerSecond{
            1}; /* the processing rate of rays for this scene object */
    };

    struct Worker {
        enum class State { Idle, Busy };
        WorkerId id;
        State state{State::Idle};
        std::shared_ptr<TCPConnection> connection;
        Optional<Address> udpAddress{};
        Optional<Bounds2i> tile;
        std::set<ObjectTypeID> objects;
        size_t freeSpace{2 * 1000 * 1000 *
                         1000}; /* in bytes (assuming 2 GBs free to start) */

        Worker(const WorkerId id, std::shared_ptr<TCPConnection> &&connection)
            : id(id), connection(std::move(connection)) {}
    };

    Poller::Action::Result::Type handleMessages();
    Poller::Action::Result::Type handleWorkerRequests();

    bool processMessage(const WorkerId workerId, const meow::Message &message);
    bool processWorkerRequest(const WorkerId workerId,
                              const meow::Message &message);
    void loadCamera();

    /* Assigning Objects */
    std::vector<ObjectTypeID> assignAllTreelets(Worker &worker);
    std::vector<ObjectTypeID> assignRootTreelet(Worker &worker);
    std::vector<ObjectTypeID> assignTreelets(Worker &worker);
    std::vector<ObjectTypeID> assignBaseSceneObjects(Worker &worker);
    void assignObject(Worker &worker, const ObjectTypeID &object);
    std::set<ObjectTypeID> getRecursiveDependencies(const ObjectTypeID &object);
    size_t getObjectSizeWithDependencies(Worker &worker,
                                         const ObjectTypeID &object);
    void updateObjectUsage(const Worker &worker);

    /* AWS Lambda */
    HTTPRequest generateRequest();

    const std::string scenePath;
    const uint32_t numberOfLambdas;
    const std::string publicAddress;
    const std::string storageBackend;
    const Address awsAddress;
    const std::string awsRegion;
    const AWSCredentials awsCredentials{};

    ExecutionLoop loop{};
    std::shared_ptr<UDPConnection> udpConnection{};

    WorkerId currentWorkerID = 0;
    std::map<WorkerId, Worker> workers{};
    std::map<TreeletId, std::vector<WorkerId>> treeletToWorker{};

    /* Message Queues */
    std::deque<std::pair<WorkerId, meow::Message>> incomingMessages;
    std::deque<std::pair<WorkerId, meow::Message>> pendingWorkerRequests;

    /* Scene Data */
    std::vector<std::unique_ptr<Transform>> transformCache{};
    std::shared_ptr<Camera> camera{};
    size_t totalPaths{0};

    /* Scene Objects */
    Bounds2i sampleBounds;
    std::map<ObjectTypeID, SceneObjectInfo> sceneObjects;
    std::map<ObjectTypeID, std::set<ObjectTypeID>> requiredDependentObjects;
    std::stack<ObjectTypeID> unassignedTreelets;

    /* Always-on FD */
    FileDescriptor dummyFD{STDOUT_FILENO};

    /* Timers */
    TimerFD workerRequestTimer;
    TimerFD statusPrintTimer;

    std::chrono::steady_clock::time_point startTime{
        std::chrono::steady_clock::now()};

    /* Worker stats */
    WorkerStats workerStats;
};

class Schedule {
  public:
  private:
};

}  // namespace pbrt

#endif /* PBRT_CLOUD_LAMBDA_MASTER_H */
