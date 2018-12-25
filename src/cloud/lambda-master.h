#ifndef PBRT_CLOUD_LAMBDA_MASTER_H
#define PBRT_CLOUD_LAMBDA_MASTER_H

#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

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
    struct Lambda {
        enum class State { Idle, Busy };
        size_t id;
        State state{State::Idle};
        std::shared_ptr<TCPConnection> connection;
        Optional<Address> udpAddress{};
        Optional<Bounds2i> tile;

        Lambda(const size_t id, std::shared_ptr<TCPConnection> &&connection)
            : id(id), connection(std::move(connection)) {}
    };

    bool process_message(const uint64_t lambdaId, const meow::Message &message);
    void loadCamera();

    std::string scenePath;
    ExecutionLoop loop{};
    uint64_t currentLambdaID = 0;
    std::map<uint64_t, Lambda> lambdas{};
    std::shared_ptr<UDPConnection> udpConnection{};
    std::string getSceneMessageStr;

    std::deque<std::pair<uint64_t, meow::Message>> incomingMessages;

    /* Scene Data */
    std::vector<std::unique_ptr<Transform>> transformCache{};
    std::shared_ptr<Camera> camera{};

    Bounds2i sampleBounds;
};

}  // namespace pbrt

#endif /* PBRT_CLOUD_LAMBDA_MASTER_H */
