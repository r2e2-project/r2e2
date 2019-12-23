#include "cloud/transfer.h"

#include "net/http_response_parser.h"

using namespace std;
using namespace chrono;
using namespace pbrt;

const static std::string UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD";

TCPSocket tcp_connection(const Address& address) {
    TCPSocket sock;
    sock.connect(address);
    return sock;
}

TransferAgent::TransferAgent(const S3StorageBackend& backend) {
    clientConfig.credentials = backend.client().credentials();
    clientConfig.region = backend.client().config().region;
    clientConfig.bucket = backend.bucket();
    clientConfig.prefix = backend.prefix();

    clientConfig.endpoint =
        S3::endpoint(clientConfig.region, clientConfig.bucket);

    clientConfig.address = Address{clientConfig.endpoint, "https"};
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

void TransferAgent::workerThread(Action&& a) {
    runningTasks++;

    Action action = move(a);
    SSLContext ssl_context;

    bool connectionOkay = true;
    bool workToDo = true;

    const milliseconds backoff{50};
    size_t tryCount = 0;

    Address address{clientConfig.endpoint, "https"};

    while (workToDo) {
        /* Did the connection fail? Pause for a moment */
        if (!connectionOkay) {
            this_thread::sleep_for(backoff * (1 << (tryCount - 1)));
            address = {clientConfig.endpoint, "https"};
        }

        tryCount = min(8ul, tryCount + 1);  // maximum is 6.4s

        /* Creating a connection to S3 */
        SecureSocket s3 =
            ssl_context.new_secure_socket(tcp_connection(address));

        try {
            s3.connect();
        } catch (exception& ex) {
            connectionOkay = false;
            continue;
        }

        HTTPResponseParser parser;

        connectionOkay = true;
        size_t requestCount = 0;
        size_t responseCount = 0;

        while (connectionOkay && workToDo) {
            HTTPRequest outgoingRequest = getRequest(action);
            parser.new_request_arrived(outgoingRequest);
            requestCount++;

            try {
                s3.write(outgoingRequest.str());
            } catch (exception& ex) {
                connectionOkay = false;
                break;
            }

            while (responseCount < requestCount) {
                try {
                    parser.parse(s3.read());
                } catch (exception& ex) {
                    connectionOkay = false;
                    break;
                }

                if (!parser.empty()) {
                    const string status = move(parser.front().status_code());
                    const string data = move(parser.front().body());
                    parser.pop();

                    responseCount++;

                    if (status != "200") {
                        if (status[0] == '5') {
                            /* 500 or 503; we need to back-off */
                            connectionOkay = false;
                            break;
                        }

                        /* it seems that it's our fault */
                        throw runtime_error("transfer failed: " + status);
                    }

                    action.data = move(data);

                    /* let's reset the try count */
                    tryCount = 0;

                    /* putting the result on the queue */
                    {
                        unique_lock<mutex> lock{resultsMutex};
                        results.emplace(move(action));
                        isEmpty = false;
                    }

                    /* is there another request that we pick up? */
                    {
                        unique_lock<mutex> lock{outstandingMutex};

                        if (!outstanding.empty()) {
                            action = move(outstanding.front());
                            outstanding.pop();
                            workToDo = true;
                        } else {
                            workToDo = false;
                        }
                    }

                    if (requestCount >= MAX_REQUESTS_ON_CONNECTION) {
                        connectionOkay = false;
                        tryCount = 1;
                    }
                }
            }
        }
    }

    runningTasks--;
}

void TransferAgent::doAction(Action&& action) {
    if (runningTasks.load() >= MAX_SIMULTANEOUS_JOBS) {
        unique_lock<mutex> lock{outstandingMutex};
        outstanding.push(move(action));
        return;
    }

    thread(&TransferAgent::workerThread, this, move(action)).detach();
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
