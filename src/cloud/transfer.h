#ifndef PBRT_CLOUD_TRANSFER_H
#define PBRT_CLOUD_TRANSFER_H

#include <atomic>
#include <condition_variable>
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

    static constexpr size_t MAX_THREADS{4};
    static constexpr size_t MAX_REQUESTS_ON_CONNECTION{8};

    std::vector<std::thread> threads{};
    std::atomic<bool> terminated{false};
    std::mutex resultsMutex;
    std::mutex outstandingMutex;
    std::condition_variable cv;

    std::queue<Action> outstanding{};
    std::queue<Action> results{};
    std::atomic<bool> isEmpty{true};

    HTTPRequest getRequest(const Action& action);
    void doAction(Action&& action);

    void workerThread(const size_t threadId);

  public:
    TransferAgent(const S3StorageBackend& backend);
    uint64_t requestDownload(const std::string& key);
    uint64_t requestUpload(const std::string& key, std::string&& data);
    ~TransferAgent();

    bool empty() const { return isEmpty.load(); }
    Action pop();
};

}  // namespace pbrt

#endif /* PBRT_CLOUD_TRANSFER_H */
