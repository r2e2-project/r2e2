#ifndef PBRT_CLOUD_LAMBDA_MASTER_H
#define PBRT_CLOUD_LAMBDA_MASTER_H

#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "cloud/lambda.h"
#include "core/camera.h"
#include "core/geometry.h"
#include "core/transform.h"
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
    struct Worker {
        enum class State { Idle, Busy };
        WorkerId id;
        State state{State::Idle};
        std::shared_ptr<TCPConnection> connection;
        Optional<Address> udpAddress{};
        Optional<Bounds2i> tile;

        Worker(const WorkerId id, std::shared_ptr<TCPConnection> &&connection)
            : id(id), connection(std::move(connection)) {}
    };

    bool processMessage(const WorkerId workerId, const meow::Message &message);
    void loadCamera();

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

    Bounds2i sampleBounds;
};

}  // namespace pbrt

#endif /* PBRT_CLOUD_LAMBDA_MASTER_H */
