#include "transfer_mcd.h"

#include "util/optional.h"

using namespace std;
using namespace chrono;

namespace memcached {

TransferAgent::TransferAgent(const vector<Address> &s, const size_t tc)
    : ::TransferAgent(),
      servers(s),
      outstandings(servers.size()),
      outstandingMutexes(servers.size()),
      cvs(servers.size()) {
    threadCount = tc;

    if (servers.size() == 0) {
        throw runtime_error("no servers specified");
    }

    if (threadCount == 0) {
        throw runtime_error("thread count cannot be zero");
    }

    for (size_t i = 0; i < threadCount; i++) {
        threads.emplace_back(&TransferAgent::workerThread, this, i);
    }
}

TransferAgent::~TransferAgent() {
    terminated = true;
    for (auto &cv : cvs) cv.notify_all();
}

size_t getHash(const string &key) {
    size_t result = 5381;
    for (const char c : key) result = ((result << 5) + result) + c;
    return result;
}

void TransferAgent::doAction(Action &&action) {
    /* what is the server id for this key? */
    const size_t serverId = getHash(action.key) % servers.size();

    {
        unique_lock<mutex> lock{outstandingMutexes[serverId]};
        outstandings[serverId].push(move(action));
    }

    cvs[serverId].notify_one();
    return;
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

    const size_t serverId = threadId % servers.size();

    const Address address = servers.at(serverId);
    auto &outstanding = outstandings.at(serverId);
    auto &outstandingMutex = outstandingMutexes.at(serverId);
    auto &cv = cvs.at(serverId);

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

                cv.wait(lock,
                        [&]() { return terminated || !outstanding.empty(); });

                if (terminated) return;

                do {
                    actions.push_back(move(outstanding.front()));
                    outstanding.pop();
                } while (false);
            }

            for (const auto &action : actions) {
                string request =
                    (action.task == Task::Download)
                        ? (GetRequest{action.key}.str() +
                           DeleteRequest{action.key}.str())
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
        }
    }
}

}  // namespace memcached
