#include "transfer_mcd.h"

#include "util/optional.h"

using namespace std;
using namespace chrono;

namespace memcached {

TransferAgent::TransferAgent(const size_t tc) : ::TransferAgent() {
    threadCount = tc;
    if (threadCount == 0) {
        throw runtime_error("thread count cannot be zero");
    }

    for (size_t i = 0; i < threadCount; i++) {
        threads.emplace_back(&TransferAgent::workerThread, this, i);
    }
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

}  // namespace memcached
