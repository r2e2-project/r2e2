#ifndef PBRT_CLOUD_TRANSFER_H
#define PBRT_CLOUD_TRANSFER_H

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <future>
#include <iterator>
#include <limits>
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

constexpr std::chrono::seconds ADDR_UPDATE_INTERVAL{25};

class TransferAgent {
  public:
    enum class Task { Download, Upload };

  private:
    struct Action {
        uint64_t id;
        Task task;
        std::string key;
        std::string data;

        Action(const uint64_t id, const Task task, const std::string& key,
               std::string&& data)
            : id(id), task(task), key(key), data(move(data)) {}
    };

    uint64_t nextId{1};

    struct S3Config {
        AWSCredentials credentials{};
        std::string region{};
        std::string bucket{};
        std::string prefix{};

        std::string endpoint{};
        std::atomic<Address> address{Address{}};
    } clientConfig;

    static constexpr size_t MAX_THREADS{8};
    static constexpr size_t MAX_REQUESTS_ON_CONNECTION{4};

    std::chrono::steady_clock::time_point lastAddrUpdate{};

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
    bool tryPop(std::pair<uint64_t, std::string>& output);

    template <class Container>
    size_t tryPopBulk(
        std::back_insert_iterator<Container> insertIt,
        const size_t maxCount = std::numeric_limits<size_t>::max());
};

template <class Container>
size_t TransferAgent::tryPopBulk(
    std::back_insert_iterator<Container> insertIt, const size_t maxCount) {
    std::unique_lock<std::mutex> lock{resultsMutex};

    if (results.empty()) return 0;

    size_t count;
    for (count = 0; !results.empty() && count < maxCount; count++) {
        insertIt = std::move(results.front());
        results.pop();
    }

    return count;
}

}  // namespace pbrt

#endif /* PBRT_CLOUD_TRANSFER_H */
