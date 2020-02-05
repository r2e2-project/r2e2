#include "transfer_mcd.h"

#include "util/optional.h"

using namespace std;
using namespace chrono;

namespace memcached {

TransferAgent::TransferAgent(const vector<Address> &s, const size_t tc,
                             const bool autoDelete)
    : ::TransferAgent(),
      servers(s),
      outstandings(servers.size()),
      outstandingMutexes(servers.size()),
      cvs(servers.size()),
      autoDelete(autoDelete) {
    threadCount = tc ? tc : servers.size() * 2;

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
    deque<Action> secondaryActions;

    while (!terminated) {
        TCPSocket sock;
        auto parser = make_unique<ResponseParser>();
        bool connectionOkay = true;

        sock.set_read_timeout(1s);
        sock.set_write_timeout(1s);

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
                string requestStr;

                if (action.task == Task::Download) {
                    auto request = GetRequest{action.key};
                    parser->new_request(request);
                    requestStr = request.str();
                } else {
                    auto request = SetRequest{action.key, action.data};
                    parser->new_request(request);
                    requestStr = request.str();
                }

                /* piggybacking of delete requests */
                if (!secondaryActions.empty()) {
                    auto &front = secondaryActions.front();

                    if (front.task == Task::Delete) {
                        auto request = DeleteRequest{front.key};
                        parser->new_request(request);
                        requestStr += request.str();
                    }

                    secondaryActions.pop_front();
                }

                TRY_OPERATION(sock.write(requestStr), break);
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

                    switch (type) {
                    case Response::Type::VALUE:
                        if (autoDelete) {
                            /* tell the memcached server to remove the object */
                            secondaryActions.emplace_back(
                                0, Task::Delete, actions.front().key, "");
                            /* fall-through */
                        }

                    case Response::Type::STORED:
                        tryCount = 0;

                        {
                            unique_lock<mutex> lock{resultsMutex};
                            results.emplace(
                                actions.front().id,
                                move(parser->front().unstructured_data()));
                        }

                        actions.pop_front();
                        eventFD.write_event();
                        break;

                    case Response::Type::NOT_STORED:
                    case Response::Type::ERROR:
                        connectionOkay = false;
                        tryCount++;
                        break;

                    case Response::Type::DELETED:
                    case Response::Type::NOT_FOUND:
                        break;

                    default:
                        throw runtime_error("transfer failed: " +
                                            parser->front().first_line());
                    }

                    parser->pop();
                }
            }
        }
    }
}

}  // namespace memcached
