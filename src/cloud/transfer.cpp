#include "cloud/transfer.h"

#include "net/http_response_parser.h"
#include "net/socket.h"
#include "util/poller.h"
#include "util/random.h"

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
        eventFds.emplace_back();
    }

    for (size_t i = 0; i < WORKER_THREAD_COUNT; i++) {
        workerThreads.emplace_back(&TransferAgent::workerThread, this, i);
    }
}

TransferAgent::~TransferAgent() {
    terminated = true;

    for (auto& e : eventFds) e.write_event();
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

void TransferAgent::workerThread(const size_t threadIndex) {
    auto& eventFd = eventFds[threadIndex];

    Poller poller;
    TCPSocket socket;
    bool connectionOkay = false;
    unique_ptr<HTTPResponseParser> parser;
    string writeBuffer;

    queue<Action> waiting;
    queue<Action> submitted;
    queue<Action> done;

    auto socketReadCallback = [&]() {
        try {
            parser->parse(socket.read());
        } catch (exception& ex) {
            connectionOkay = false;
            return ResultType::CancelAll;
        }

        while (!parser->empty()) {
            const string status = move(parser->front().status_code());
            const string data = move(parser->front().body());
            parser->pop();

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
    };

    auto socketWriteCallback = [&]() {
        string::const_iterator next;

        try {
            next = socket.write(writeBuffer.cbegin(), writeBuffer.cend());
        } catch (exception& ex) {
            connectionOkay = false;
            return ResultType::CancelAll;
        }

        writeBuffer.erase(writeBuffer.cbegin(), next);
        return ResultType::Continue;
    };

    /* work is read from `outstanding` */
    poller.add_action(Poller::Action(
        eventFd, Direction::In,
        [&]() {
            if (terminated) return ResultType::Exit;

            if (eventFd.read_event()) {
                unique_lock<mutex> lock{outstandingMutex};
                if (!this->outstanding.empty()) {
                    waiting.push(move(this->outstanding.front()));
                    this->outstanding.pop();
                }
            }

            return ResultType::Continue;
        },
        []() { return true; },
        []() { throw runtime_error("event fd failed"); }));

    poller.add_action(Poller::Action(
        alwaysOnFd, Direction::Out,
        [&] {
            HTTPRequest outgoingRequest = getRequest(waiting.front());
            parser->new_request_arrived(outgoingRequest);
            writeBuffer += outgoingRequest.str();
            submitted.push(move(waiting.front()));
            waiting.pop();

            return ResultType::Continue;
        },
        [&]() { return !waiting.empty(); },
        []() { throw runtime_error("waiting queue failed"); }));

    /* writing back the output */
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
        if (!connectionOkay) {
            /* we need to set up the connection */
            socket = {};
            socket.set_blocking(false);
            socket.connect_nonblock(clientConfig.address);
            parser = make_unique<HTTPResponseParser>();

            waiting = move(submitted);
            submitted = {};
            writeBuffer = {};

            /* doing the transfer -- write */
            poller.add_action(Poller::Action(
                socket, Direction::Out, socketWriteCallback,
                [&writeBuffer]() { return !writeBuffer.empty(); },
                [&]() { connectionOkay = false; }));

            /* doing the transfer -- read */
            poller.add_action(Poller::Action(
                socket, Direction::In, socketReadCallback,
                []() { return true; }, [&]() { connectionOkay = false; }));

            connectionOkay = true;
        }

        if (poller.poll(-1).result == Poller::Result::Type::Exit) break;
    }
}

void TransferAgent::doAction(Action&& action) {
    {
        unique_lock<mutex> lock{outstandingMutex};
        outstanding.push(move(action));
    }

    random::sample(eventFds.begin(), eventFds.end())->write_event();
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
