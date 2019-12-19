#ifndef PBRT_CLOUD_TRANSFER_H
#define PBRT_CLOUD_TRANSFER_H

#include <atomic>
#include <cstdint>
#include <future>
#include <map>
#include <mutex>
#include <queue>
#include <string>

#include "net/address.h"
#include "net/s3.h"
#include "net/secure_socket.h"
#include "net/socket.h"
#include "storage/backend_s3.h"

namespace pbrt {

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

}  // namespace pbrt

#endif /* PBRT_CLOUD_TRANSFER_H */
