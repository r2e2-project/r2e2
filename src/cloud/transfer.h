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
#include "util/eventfd.h"
#include "util/optional.h"

namespace pbrt {

class TransferAgent {
  private:
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

    uint64_t nextId{1};

    struct S3Config {
        AWSCredentials credentials{};
        std::string region{};
        std::string bucket{};
        std::string prefix{};

        std::string endpoint{};
        Address address{};
    } clientConfig;

    static constexpr size_t MAX_THREADS{8};
    static constexpr size_t MAX_REQUESTS_ON_CONNECTION{4};

    std::vector<std::thread> threads{};
    std::atomic<bool> terminated{false};
    std::mutex resultsMutex;
    std::mutex outstandingMutex;
    std::condition_variable cv;

    std::queue<Action> outstanding{};
    std::queue<std::pair<uint64_t, std::string>> results{};

    HTTPRequest getRequest(const Action& action);
    void doAction(Action&& action);

    void workerThread(const size_t threadId);

    EventFD eventFD{false};

  public:
    TransferAgent(const S3StorageBackend& backend);
    uint64_t requestDownload(const std::string& key);
    uint64_t requestUpload(const std::string& key, std::string&& data);
    ~TransferAgent();

    EventFD& eventfd() { return eventFD; }

    bool empty() const;
    Optional<std::pair<uint64_t, std::string>> try_pop();
};

}  // namespace pbrt

#endif /* PBRT_CLOUD_TRANSFER_H */
