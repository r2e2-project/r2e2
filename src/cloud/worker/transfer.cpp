#include "cloud/lambda-worker.h"

using namespace std;
using namespace pbrt;
using namespace PollerShortNames;

const static std::string UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD";

ResultType LambdaWorker::handleSendQueue() {
    sendQueueTimer.reset();

    while (!sendQueue.empty()) {
        auto queuekv = sendQueue.begin();
        const auto treeletId = queuekv->first;
        auto& queue = queuekv->second;

        while (!queue.empty()) {
            auto& bag = queue.front();

            const auto bagId = currentBagId[treeletId]++;
            const string key = rayBagKey(treeletId, bagId);
            const auto id = transferAgent.requestUpload(key, move(bag.second));
            pendingRayBags[id] = {treeletId, bagId, bag.first};

            queue.pop();
        }
    }

    return ResultType::Continue;
}

ResultType LambdaWorker::handleTransferResults() {
    while (!transferAgent.empty()) {
        TransferAgent::Action action = move(transferAgent.pop());

        if (pendingRayBags.count(action.id)) {
            auto rayBagId = &pendingRayBags[action.id];

            /* tell the master we've finished uploading this */
        }
    }

    return ResultType::Continue;
}

TCPSocket tcp_connection(const Address& address) {
    TCPSocket sock;
    sock.connect(address);
    return sock;
}

LambdaWorker::TransferAgent::TransferAgent(const S3StorageBackend& backend) {
    clientConfig.credentials = backend.client().credentials();
    clientConfig.region = backend.client().config().region;
    clientConfig.bucket = backend.bucket();
    clientConfig.prefix = backend.prefix();

    clientConfig.endpoint =
        S3::endpoint(clientConfig.region, clientConfig.bucket);

    clientConfig.address = Address{clientConfig.endpoint, "https"};
}

void LambdaWorker::TransferAgent::doAction(Action&& action) {
    runningTasks.emplace(make_pair(
        action.id, async(launch::async, [this, a{move(action)}] {
            Action action = move(a);
            const auto& config = this->clientConfig;

            SSLContext ssl_context;
            HTTPResponseParser responses;
            SecureSocket s3 =
                ssl_context.new_secure_socket(tcp_connection(config.address));

            s3.connect();

            HTTPRequest outgoingRequest;

            switch (action.type) {
            case Action::Download:
                outgoingRequest =
                    S3PutRequest(config.credentials, config.endpoint,
                                 config.region, action.key, action.data,
                                 UNSIGNED_PAYLOAD)
                        .to_http_request();
                break;

            case Action::Upload:
                outgoingRequest =
                    S3GetRequest(config.credentials, config.endpoint,
                                 config.region, action.key)
                        .to_http_request();
                break;
            }

            responses.new_request_arrived(outgoingRequest);
            s3.write(outgoingRequest.str());

            while (responses.pending_requests()) {
                responses.parse(s3.read());

                if (!responses.empty()) {
                    if (responses.front().first_line() != "HTTP/1.1 200 OK") {
                        throw runtime_error("TransferAgent::doAction failed ");
                    } else {
                        action.data = move(responses.front().body());
                        break;
                    }
                }
            }

            {
                unique_lock<mutex> lock{this->mtx};
                this->isEmpty = false;
                results.emplace(move(action));
            }
        })));
}

uint64_t LambdaWorker::TransferAgent::requestDownload(const string& key) {
    doAction({nextId, Action::Download, key, string()});
    return nextId++;
}

uint64_t LambdaWorker::TransferAgent::requestUpload(const string& key,
                                                    string&& data) {
    doAction({nextId, Action::Upload, key, move(data)});
    return nextId++;
}

bool LambdaWorker::TransferAgent::empty() { return isEmpty.load(); }

LambdaWorker::TransferAgent::Action LambdaWorker::TransferAgent::pop() {
    unique_lock<mutex> lock{mtx};
    Action action = move(results.front());
    results.pop();

    runningTasks.erase(action.id);
    isEmpty.store(results.empty());

    return action;
}
