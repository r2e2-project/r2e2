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

    while (workToDo) {
        /* Did the connection fail? Pause for a moment */
        if (!connectionOkay) this_thread::sleep_for(milliseconds{50});

        /* Creating a connection to S3 */
        SecureSocket s3 =
            ssl_context.new_secure_socket(tcp_connection(clientConfig.address));

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
                string data;

                try {
                    data = s3.read();
                } catch (exception& ex) {
                    connectionOkay = false;
                    break;
                }

                parser.parse(data);

                if (!parser.empty()) {
                    const string status = move(parser.front().first_line());
                    const string data = move(parser.front().body());
                    parser.pop();

                    responseCount++;

                    if (status != "HTTP/1.1 200 OK") {
                        cerr << "transfer failed: " << status << endl;
                        connectionOkay = false;
                        break;
                    }

                    action.data = move(data);

                    /* putting the result on the queue */
                    {
                        unique_lock<mutex> lock{resultsMutex};
                        isEmpty = false;
                        results.emplace(move(action));
                    }

                    /* is there another request that we pick up? */
                    {
                        unique_lock<mutex> lock{outstandingMutex};

                        if (!outstanding.empty()) {
                            action = move(outstanding.front());
                            outstanding.pop();
                        } else {
                            workToDo = false;
                        }
                    }

                    if (requestCount >= MAX_REQUESTS_ON_CONNECTION) {
                        connectionOkay = false;
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
