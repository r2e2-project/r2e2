#include "cloud/transfer.h"

#include "net/http_response_parser.h"
#include "util/optional.h"

using namespace std;
using namespace chrono;
using namespace pbrt;

const static std::string UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD";

TransferAgent::TransferAgent(const unique_ptr<StorageBackend>& backend,
                             const size_t threadCount)
    : threadCount(threadCount) {
    if (threadCount == 0) {
        throw runtime_error("thread count cannot be zero");
    }

    auto s3Backend = dynamic_cast<S3StorageBackend*>(backend.get());

    if (s3Backend != nullptr) {
        clientConfig.credentials = s3Backend->client().credentials();
        clientConfig.region = s3Backend->client().config().region;
        clientConfig.bucket = s3Backend->bucket();
        clientConfig.prefix = s3Backend->prefix();

        clientConfig.endpoint =
            S3::endpoint(clientConfig.region, clientConfig.bucket);

        clientConfig.address.store(Address{clientConfig.endpoint, "http"});
    } else {
        auto gsBackend = dynamic_cast<GoogleStorageBackend*>(backend.get());

        if (gsBackend != nullptr) {
            clientConfig.credentials = gsBackend->client().credentials();
            clientConfig.region = gsBackend->client().config().region;
            clientConfig.bucket = gsBackend->bucket();
            clientConfig.prefix = gsBackend->prefix();
            clientConfig.endpoint = gsBackend->client().config().endpoint;
            clientConfig.address.store(Address{clientConfig.endpoint, "http"});
        } else {
            throw runtime_error("unsupported backend");
        }
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

HTTPRequest TransferAgent::getRequest(const Action& action) {
    switch (action.task) {
    case Task::Upload:
        return S3PutRequest(clientConfig.credentials, clientConfig.endpoint,
                            clientConfig.region, action.key, action.data,
                            UNSIGNED_PAYLOAD)
            .to_http_request();

    case Task::Download:
        return S3GetRequest(clientConfig.credentials, clientConfig.endpoint,
                            clientConfig.region, action.key)
            .to_http_request();

    default:
        throw runtime_error("Unknown action task");
    }
}

#define TRY_OPERATION(x, y)     \
    try {                       \
        x;                      \
    } catch (exception & ex) {  \
        tryCount++;             \
        connectionOkay = false; \
        s3.close();             \
        y;                      \
    }

void TransferAgent::workerThread(const size_t threadId) {
    constexpr milliseconds backoff{50};
    size_t tryCount = 0;

    deque<Action> actions;

    auto lastAddrUpdate = steady_clock::now() + seconds{threadId};
    Address s3Address = clientConfig.address.load();

    while (!terminated) {
        TCPSocket s3;
        auto parser = make_unique<HTTPResponseParser>();
        bool connectionOkay = true;
        size_t requestCount = 0;

        if (tryCount > 0) {
            tryCount = min<size_t>(tryCount, 7u);  // caps at 3.2s
            this_thread::sleep_for(backoff * (1 << (tryCount - 1)));
        }

        if (steady_clock::now() - lastAddrUpdate >= ADDR_UPDATE_INTERVAL) {
            s3Address = clientConfig.address.load();
            lastAddrUpdate = steady_clock::now();
        }

        TRY_OPERATION(s3.connect(s3Address), continue);

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
                HTTPRequest request = getRequest(action);
                parser->new_request_arrived(request);
                TRY_OPERATION(s3.write(request.str()), break);
                requestCount++;
            }

            while (!terminated && connectionOkay && !actions.empty()) {
                string result;
                TRY_OPERATION(result = s3.read(), break);

                if (result.length() == 0) {
                    // connection was closed by the other side
                    tryCount++;
                    connectionOkay = false;
                    s3.close();
                    break;
                }

                parser->parse(result);

                while (!parser->empty()) {
                    const string status = move(parser->front().status_code());
                    const string data = move(parser->front().body());
                    parser->pop();

                    switch (status[0]) {
                    case '2':  // successful
                    {
                        unique_lock<mutex> lock{resultsMutex};
                        results.emplace(actions.front().id, move(data));
                    }

                        tryCount = 0;
                        actions.pop_front();
                        eventFD.write_event();
                        break;

                    case '5':  // we need to slow down
                        connectionOkay = false;
                        tryCount++;
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
    if (steady_clock::now() - lastAddrUpdate >= ADDR_UPDATE_INTERVAL) {
        clientConfig.address.store(Address{clientConfig.endpoint, "http"});
        lastAddrUpdate = steady_clock::now();
    }

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
