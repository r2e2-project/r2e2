#include "memcached.h"

#include "util/optional.h"

using namespace std;
using namespace chrono;
using namespace memcached;

TransferAgent::TransferAgent(const size_t threadCount)
    : threadCount(threadCount) {
    if (threadCount == 0) {
        throw runtime_error("thread count cannot be zero");
    }

    for (size_t i = 0; i < threadCount; i++) {
        threads.emplace_back(&TransferAgent::workerThread, this, i);
    }
}

TransferAgent::~TransferAgent() {
    terminated = true;
    cv.notify_all();
    for (auto& t : threads) t.join();
}

#define TRY_OPERATION(x, y)     \
    try {                       \
        x;                      \
    } catch (exception & ex) {  \
        tryCount++;             \
        connectionOkay = false; \
        sock.close();           \
        y;                      \
    }

void TransferAgent::workerThread(const size_t threadId) {
    constexpr milliseconds backoff{50};
    size_t tryCount = 0;

    deque<Action> actions;

    while (!terminated) {
        TCPSocket sock;
        auto parser = make_unique<ResponseParser>();
        bool connectionOkay = true;
        size_t requestCount = 0;

        if (tryCount > 0) {
            tryCount = min<size_t>(tryCount, 7u);  // caps at 3.2s
            this_thread::sleep_for(backoff * (1 << (tryCount - 1)));
        }

        TRY_OPERATION(sock.connect(address), continue);

        while (!terminated && connectionOkay) {
            /* make sure we have an action to perfom */
            if (actions.empty()) {
                unique_lock<mutex> lock{outstandingMutex};

                cv.wait(lock, [this]() {
                    return terminated || !outstanding.empty();
                });

                if (terminated) return;

                const auto capacity = MAX_REQUESTS_ON_CONNECTION - requestCount;

                do {
                    actions.push_back(move(outstanding.front()));
                    outstanding.pop();
                } while (!outstanding.empty() &&
                         outstanding.size() / threadCount >= 1 &&
                         actions.size() < capacity);
            }

            for (const auto& action : actions) {
                string request =
                    (action.task == Task::Download)
                        ? GetRequest{action.key}.str() +
                              DeleteRequest{action.key}.str()
                        : SetRequest{action.key, action.data}.str();

                TRY_OPERATION(sock.write(request), break);
                requestCount++;
            }

            while (!terminated && connectionOkay && !actions.empty()) {
                string result;
                TRY_OPERATION(result = sock.read(), break);

                if (result.length() == 0) {
                    // connection was closed by the other side
                    tryCount++;
                    connectionOkay = false;
                    sock.close();
                    break;
                }

                parser->parse(result);

                while (!parser->empty()) {
                    const auto type = parser->front().type();
                    const auto status = move(parser->front().first_line());
                    const auto data = move(parser->front().unstructured_data());

                    parser->pop();

                    switch (type) {
                    case Response::Type::STORED:
                    case Response::Type::VALUE: {
                        unique_lock<mutex> lock{resultsMutex};
                        results.emplace(actions.front().id, move(data));
                    }

                        tryCount = 0;
                        actions.pop_front();
                        eventFD.write_event();
                        break;

                    default:  // unexpected response, like 404 or something
                        throw runtime_error("transfer failed: " + status);
                    }
                }
            }

            if (requestCount >= MAX_REQUESTS_ON_CONNECTION) {
                connectionOkay = false;
            }
        }
    }
}

void TransferAgent::doAction(Action&& action) {
    {
        unique_lock<mutex> lock{outstandingMutex};
        outstanding.push(move(action));
    }

    cv.notify_one();
    return;
}

uint64_t TransferAgent::requestDownload(const string& key) {
    doAction({nextId, Task::Download, key, string()});
    return nextId++;
}

uint64_t TransferAgent::requestUpload(const string& key, string&& data) {
    doAction({nextId, Task::Upload, key, move(data)});
    return nextId++;
}

bool TransferAgent::tryPop(pair<uint64_t, string>& output) {
    unique_lock<mutex> lock{resultsMutex};

    if (results.empty()) return false;

    output = move(results.front());
    results.pop();

    return true;
}
