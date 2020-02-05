#ifndef PBRT_CNET_TRANSFER_H
#define PBRT_CNET_TRANSFER_H

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
#include "net/socket.h"
#include "util/eventfd.h"
#include "util/optional.h"

class TransferAgent {
  public:
    enum class Task { Download, Upload, Delete };

  protected:
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

    static constexpr size_t MAX_THREADS{8};
    size_t threadCount{MAX_THREADS};

    std::vector<std::thread> threads{};
    std::atomic<bool> terminated{false};
    std::mutex resultsMutex{};
    std::mutex outstandingMutex{};
    std::condition_variable cv{};

    std::queue<Action> outstanding{};
    std::queue<std::pair<uint64_t, std::string>> results{};

    EventFD eventFD{false};

    virtual void doAction(Action&& action);
    virtual void workerThread(const size_t threadId) = 0;

  public:
    TransferAgent() {}

    uint64_t requestDownload(const std::string& key);
    uint64_t requestUpload(const std::string& key, std::string&& data);
    virtual ~TransferAgent();

    EventFD& eventfd() { return eventFD; }

    bool empty() const;
    bool tryPop(std::pair<uint64_t, std::string>& output);

    template <class Container>
    size_t tryPopBulk(
        std::back_insert_iterator<Container> insertIt,
        const size_t maxCount = std::numeric_limits<size_t>::max());
};

template <class Container>
size_t TransferAgent::tryPopBulk(std::back_insert_iterator<Container> insertIt,
                                 const size_t maxCount) {
    std::unique_lock<std::mutex> lock{resultsMutex};

    if (results.empty()) return 0;

    size_t count;
    for (count = 0; !results.empty() && count < maxCount; count++) {
        insertIt = std::move(results.front());
        results.pop();
    }

    return count;
}

#endif /* PBRT_NET_TRANSFER_H */
