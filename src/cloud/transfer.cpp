#include "cloud/transfer.h"

#include "net/http_response_parser.h"
#include "net/socket.h"
#include "util/poller.h"

using namespace std;
using namespace chrono;
using namespace pbrt;
using namespace PollerShortNames;

const static std::string UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD";

TransferAgent::TransferAgent(const S3StorageBackend& backend) {
    clientConfig.credentials = backend.client().credentials();
    clientConfig.region = backend.client().config().region;
    clientConfig.bucket = backend.bucket();
    clientConfig.prefix = backend.prefix();

    clientConfig.endpoint =
        S3::endpoint(clientConfig.region, clientConfig.bucket);

    clientConfig.address = Address{clientConfig.endpoint, "http"};

    for (size_t i = 0; i < WORKER_THREAD_COUNT; i++) {
        workerThreads.emplace_back(&TransferAgent::workerThread, this);
    }
}

TransferAgent::~TransferAgent() {
    terminated = true;
    workEvent.write_event();

    for (auto& t : workerThreads) t.join();
}

HTTPRequest TransferAgent::getRequest(const Action& action) {
    switch (action.type) {
    case Action::Upload:
        return S3PutRequest(clientConfig.credentials, clientConfig.endpoint,
                            clientConfig.region, action.key, action.data,
                            UNSIGNED_PAYLOAD)
            .to_http_request();

    case Action::Download:
        return S3GetRequest(clientConfig.credentials, clientConfig.endpoint,
                            clientConfig.region, action.key)
            .to_http_request();
    default:
        throw runtime_error("Unknown action type");
    }
}

void TransferAgent::workerThread() {
    Poller poller;

    SSLContext sslContext;
    TCPSocket socket;
    socket.set_blocking(false);
    socket.connect_nonblock(clientConfig.address);

    HTTPResponseParser parser;

    queue<Action> submitted;
    queue<Action> done;

    string writeBuffer;

    /* (1) work is read from `outstanding` */
    poller.add_action(Poller::Action(
        workEvent, Direction::In,
        [&]() {
            if (terminated) return ResultType::Exit;

            if (workEvent.read_event()) {
                {
                    unique_lock<mutex> lock{outstandingMutex};
                    if (!this->outstanding.empty()) {
                        submitted.push(move(this->outstanding.front()));
                        this->outstanding.pop();
                    }
                }

                HTTPRequest request = getRequest(submitted.front());
                parser.new_request_arrived(request);
                writeBuffer += request.str();
            }

            return ResultType::Continue;
        },
        []() { return true; },
        []() { throw runtime_error("event fd failed"); }));

    /* (2) doing the transfer */
    poller.add_action(Poller::Action(
        socket, Direction::Out,
        [&]() {
            auto next = socket.write(writeBuffer.cbegin(), writeBuffer.cend());
            writeBuffer.erase(writeBuffer.cbegin(), next);
            return ResultType::Continue;
        },
        [&writeBuffer]() { return !writeBuffer.empty(); },
        []() { throw runtime_error("connection failed"); }));

    /* (2) doing the transfer */
    poller.add_action(Poller::Action(
        socket, Direction::In,
        [&]() {
            parser.parse(socket.read());

            while (!parser.empty()) {
                const string status = move(parser.front().status_code());
                const string data = move(parser.front().body());
                parser.pop();

                if (status[0] != '2') {
                    if (status[0] == '5') {
                        /* XXX we need to back off */
                        submitted.push(move(submitted.front()));
                        submitted.pop();
                        return ResultType::Continue;
                    }

                    return ResultType::Exit; /* we fucked up */
                }

                if (submitted.front().type == Action::Download) {
                    submitted.front().data = move(data);
                }

                done.emplace(move(submitted.front()));
                submitted.pop();
            }

            return ResultType::Continue;
        },
        []() { return true; },
        []() { throw runtime_error("connection failed"); }));

    /* (2) writing back the output */
    poller.add_action(Poller::Action(
        alwaysOnFd, Direction::Out,
        [&]() {
            this->results.push(move(done.front()));
            done.pop();
            resultsMutex.unlock();
            isEmpty = false;

            return ResultType::Continue;
        },
        [this, &done]() { return !done.empty() && resultsMutex.try_lock(); },
        []() { throw runtime_error("always on fd failed"); }));

    while (!terminated) {
        if (poller.poll(-1).result == Poller::Result::Type::Exit) break;
    }
}

void TransferAgent::doAction(Action&& action) {
    {
        unique_lock<mutex> lock{outstandingMutex};
        outstanding.push(move(action));
    }

    workEvent.write_event();
}

uint64_t TransferAgent::requestDownload(const string& key) {
    doAction({nextId, Action::Download, key, string()});
    return nextId++;
}

uint64_t TransferAgent::requestUpload(const string& key, string&& data) {
    doAction({nextId, Action::Upload, key, move(data)});
    return nextId++;
}

bool TransferAgent::empty() { return isEmpty.load(); }

TransferAgent::Action TransferAgent::pop() {
    unique_lock<mutex> lock{resultsMutex};

    Action action = move(results.front());
    results.pop();
    isEmpty.store(results.empty());

    return action;
}
