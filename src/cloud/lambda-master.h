#ifndef PBRT_CLOUD_LAMBDA_MASTER_H
#define PBRT_CLOUD_LAMBDA_MASTER_H

#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>
#include <stack>

#include "cloud/lambda.h"
#include "core/camera.h"
#include "core/geometry.h"
#include "core/transform.h"
#include "cloud/manager.h"
#include "execution/connection.h"
#include "execution/loop.h"
#include "execution/meow/message.h"
#include "util/optional.h"

namespace pbrt {

class LambdaMaster {
  public:
    LambdaMaster(const std::string &scenePath, const uint16_t listenPort);

    void run();

    static constexpr int TILE_SIZE = 16;

  private:
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
        std::set<SceneManager::ObjectTypeID> objects;
        size_t freeSpace{2 * 1000 * 1000 *
                         1000}; /* in bytes (assuming 2 GBs free to start) */

        Worker(const WorkerId id, std::shared_ptr<TCPConnection> &&connection)
            : id(id), connection(std::move(connection)) {}
    };

    bool processMessage(const WorkerId workerId, const meow::Message &message);
    void loadCamera();

    std::vector<SceneManager::ObjectTypeID> assignAllTreelets(Worker &worker);
    std::vector<SceneManager::ObjectTypeID> assignRootTreelet(Worker &worker);
    std::vector<SceneManager::ObjectTypeID> assignTreelets(Worker &worker);
    std::vector<SceneManager::ObjectTypeID> assignBaseSceneObjects(
        Worker &worker);
    void assignObject(Worker &worker, const SceneManager::ObjectTypeID &object);
    std::set<SceneManager::ObjectTypeID> getRecursiveDependencies(
        const SceneManager::ObjectTypeID &object);
    size_t getObjectSizeWithDependencies(
        Worker &worker, const SceneManager::ObjectTypeID &object);
    void updateObjectUsage(const Worker &worker);

    std::string scenePath;
    ExecutionLoop loop{};
    std::shared_ptr<UDPConnection> udpConnection{};

    WorkerId currentWorkerID = 0;
    std::map<WorkerId, Worker> workers{};
    std::map<TreeletId, std::vector<WorkerId>> treeletToWorker{};

    std::deque<std::pair<WorkerId, meow::Message>> incomingMessages;

    /* Scene Data */
    std::string getSceneMessageStr{};
    std::vector<std::unique_ptr<Transform>> transformCache{};
    std::shared_ptr<Camera> camera{};

    /* Scene Objects */
    std::map<SceneManager::ObjectTypeID, SceneObjectInfo> sceneObjects;
    std::map<SceneManager::ObjectTypeID, std::set<SceneManager::ObjectTypeID>>
        requiredDependentObjects;
    std::stack<SceneManager::ObjectTypeID> unassignedTreelets;

    Bounds2i sampleBounds;
};

class Schedule {
  public:

  private:
};

}  // namespace pbrt

#endif /* PBRT_CLOUD_LAMBDA_MASTER_H */
