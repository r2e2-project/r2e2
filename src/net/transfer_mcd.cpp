#include "transfer_mcd.h"

#include <chrono>
#include <functional>
#include <list>

#include "util/optional.h"
#include "util/poller.h"

using namespace std;
using namespace chrono;
using namespace PollerShortNames;

namespace memcached {

TransferAgent::TransferAgent(const vector<Address> &s, const size_t tc,
                             const bool autoDelete)
    : ::TransferAgent(),
      servers(s),
      workEvents(tc),
      autoDelete(autoDelete),
      outstandings(tc),
      outstandingMutexes(tc) {
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

    for (auto &event : workEvents) {
        event.write_event();
    }
}

size_t getHash(const string &key) {
    size_t result = 5381;
    for (const char c : key) result = ((result << 5) + result) + c;
    return result;
}

void TransferAgent::doAction(Action &&action) {
    /* find out which thread is responsible for this action */
    const size_t serverIdx = getHash(action.key) % servers.size();
    const size_t threadIdx = serverIdx % threadCount;

    {
        unique_lock<mutex> lock{outstandingMutexes[threadIdx]};
        outstandings[threadIdx].push(move(action));
    }

    workEvents[threadIdx].write_event();
}

void TransferAgent::workerThread(const size_t threadId) {
    FileDescriptor alwaysOnFd{CheckSystemCall("dup", dup(STDOUT_FILENO))};
    Poller poller;

    constexpr milliseconds BASE_BACKOFF{50};

    vector<TCPSocket> sockets(servers.size());
    vector<queue<Action>> waitings(servers.size());
    vector<queue<Action>> submitteds(servers.size());
    vector<string> buffers(servers.size());
    vector<ResponseParser> parsers(servers.size());

    queue<pair<steady_clock::time_point, size_t>> connectionsToRestart;
    queue<pair<uint64_t, string>> localResults;

    unique_ptr<function<void(const size_t)>> errorCallback;

    auto socketReadCallback = [&](const size_t idx) {
        parsers[idx].parse(sockets[idx].read());
        return ResultType::Continue;
    };

    auto socketWriteCallback = [&](const size_t idx) {
        auto &buffer = buffers[idx];
        auto nextIt = sockets[idx].write(buffer.begin(), buffer.end());
        buffer.erase(buffer.begin(), nextIt);
        return ResultType::Continue;
    };

    auto newResponseCallback = [&](const size_t idx) {
        auto &waiting = waitings[idx];
        auto &submitted = submitteds[idx];

        auto response = move(parsers[idx].front());
        parsers[idx].pop();

        switch (response.type()) {
        case Response::Type::VALUE: {  // telling memcached to remove the object
            auto request = DeleteRequest{submitted.front().key};
            parsers[idx].new_request(request);
            buffers[idx] += request.str();
            /* fall through */
        }

        case Response::Type::STORED:
            /* XXX check if the response data length is zero */
            localResults.emplace(submitted.front().id,
                                 move(response.unstructured_data()));

            submitted.pop();
            break;

        case Response::Type::NOT_STORED:
        case Response::Type::ERROR:
            waiting.push(move(submitted.front()));
            submitted.pop();
            break;

        case Response::Type::DELETED:
        case Response::Type::NOT_FOUND:
            /* we don't really care */
            break;

        default:
            throw runtime_error("transfer failed: " + response.first_line());
        }

        return ResultType::Continue;
    };

    auto newWorkCallback = [&]() {
        workEvents[threadId].read_event();

        unique_lock<mutex> lock{outstandingMutexes[threadId]};
        auto &outstanding = outstandings[threadId];

        while (!outstanding.empty()) {
            auto &action = outstanding.front();
            auto serverId = getHash(action.key) % servers.size();
            auto index = (serverId - threadId) / threadCount;
            waitings[index].push(move(action));
            outstanding.pop();
        }

        return ResultType::Continue;
    };

    auto submitWorkCallback = [&](const size_t idx) {
        auto &buffer = buffers[idx];
        auto &parser = parsers[idx];
        auto &waiting = waitings[idx];
        auto &submitted = submitteds[idx];

        auto &action = waiting.front();

        if (action.task == Task::Download) {
            auto request = GetRequest{action.key};
            parser.new_request(request);
            buffer += request.str();
        } else {
            auto request = SetRequest{action.key, action.data};
            parser.new_request(request);
            buffer += request.str();
        }

        submitted.push(move(waiting.front()));
        waiting.pop();

        return ResultType::Continue;
    };

    auto resultsCallback = [&]() {
        {
            unique_lock<mutex> lock{resultsMutex};

            while (!localResults.empty()) {
                results.push(move(localResults.front()));
                localResults.pop();
            }
        }

        eventFD.write_event();

        return ResultType::Continue;
    };

    auto setupSocket = [&](const size_t i) {
        buffers[i].clear();
        parsers[i] = {};

        auto &waiting = waitings[i];
        auto &submitted = submitteds[i];

        while (!submitted.empty()) {
            waiting.push(move(submitted.front()));
            submitted.pop();
        }

        sockets[i] = {};
        sockets[i].set_blocking(false);
        sockets[i].connect_nonblock(servers[i]);

        poller.add_action(Poller::Action(
            sockets[i], Direction::In, bind(socketReadCallback, i),
            [] { return true; }, bind(*errorCallback, i)));

        poller.add_action(Poller::Action(
            sockets[i], Direction::Out, bind(socketWriteCallback, i),
            [&, idx = i] { return !buffers[idx].empty(); },
            bind(*errorCallback, i)));

        poller.add_action(Poller::Action(
            alwaysOnFd, Direction::Out, bind(newResponseCallback, i),
            [&, idx = i] { return !parsers[idx].empty(); }));

        poller.add_action(Poller::Action(
            alwaysOnFd, Direction::Out, bind(submitWorkCallback, i),
            [&, idx = i] { return !waitings[idx].empty(); }));
    };

    errorCallback =
        make_unique<function<void(const size_t)>>([&](const size_t idx) {
            connectionsToRestart.emplace(steady_clock::now() + BASE_BACKOFF,
                                         idx);
        });

    poller.add_action(Poller::Action(
        alwaysOnFd, Direction::Out, [] { return ResultType::Exit; },
        [&] { return terminated.load(); }));

    poller.add_action(Poller::Action(alwaysOnFd, Direction::Out,
                                     resultsCallback,
                                     [&] { return !localResults.empty(); }));

    poller.add_action(Poller::Action(workEvents[threadId], Direction::In,
                                     newWorkCallback, [] { return true; }));

    for (size_t i = 0; i < servers.size(); i++) {
        connectionsToRestart.emplace(steady_clock::time_point::min(), i);
    }

    while (!terminated) {
        while (!connectionsToRestart.empty() &&
               connectionsToRestart.front().first <= steady_clock::now()) {
            setupSocket(connectionsToRestart.front().second);
            connectionsToRestart.pop();
        }

        if (poller.poll(-1).result == Poller::Result::Type::Exit) break;
    }
}

}  // namespace memcached
