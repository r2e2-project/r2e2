#include "cloud/lambda-worker.h"

#include "messages/utils.h"

using namespace std;
using namespace pbrt;
using namespace meow;
using namespace PollerShortNames;

using OpCode = Message::OpCode;

const static std::string UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD";

ResultType LambdaWorker::handleOutQueue() {
    for (auto it = outQueue.begin(); it != outQueue.end();
         it = outQueue.erase(it)) {
        const TreeletId treeletId = it->first;
        auto& rayList = it->second;
        auto& queue = sendQueue[treeletId];

        while (!rayList.empty()) {
            if (queue.empty() ||
                queue.back().first + RayState::MaxCompressedSize() >
                    MAX_BAG_SIZE) {
                queue.emplace(make_pair(0, string(MAX_BAG_SIZE, '\0')));
            }

            auto& bag = queue.back();
            auto& ray = rayList.front();

            const auto len = ray->Serialize(&bag.second[0] + bag.first);
            bag.first += len;

            rayList.pop_front();
        }
    }

    return ResultType::Continue;
}

ResultType LambdaWorker::handleSendQueue() {
    sendQueueTimer.reset();

    for (auto it = sendQueue.begin(); it != sendQueue.end();
         it = sendQueue.erase(it)) {
        const auto treeletId = it->first;
        auto& queue = it->second;

        while (!queue.empty()) {
            pair<size_t, string>& item = queue.front();
            item.second.erase(item.first);

            const auto bagId = currentBagId[treeletId]++;
            const RayBagKey key{*workerId, treeletId, bagId, item.first};

            const auto id = transferAgent.requestUpload(
                key.str(rayBagsKeyPrefix), move(item.second));

            pendingRayBags[id] = make_pair(Task::Upload, key);
            queue.pop();
        }
    }

    return ResultType::Continue;
}

ResultType LambdaWorker::handleReceiveQueue() {
    while (!receiveQueue.empty()) {
        RayBag bag = move(receiveQueue.front());
        receiveQueue.pop();

        /* (1) XXX do we have this treelet? */

        /* (2) let's unpack this treelet and add the rays to the trace queue */
        const char* data = bag.data.data();

        for (size_t offset = 0; offset < bag.data.size();) {
            const auto len = *reinterpret_cast<const uint32_t*>(data + offset);
            offset += 4;

            RayStatePtr ray = RayState::Create();
            ray->Deserialize(data + offset, len);
            ray->hop++;
            offset += len;

            pushTraceQueue(move(ray));
        }
    }

    return ResultType::Continue;
}

ResultType LambdaWorker::handleTransferResults() {
    protobuf::RayBagKeys enqueuedProto;

    while (!transferAgent.empty()) {
        TransferAgent::Action action = move(transferAgent.pop());

        auto keyIt = pendingRayBags.find(action.id);
        if (keyIt != pendingRayBags.end()) {
            const auto& bag = keyIt->second.second;

            switch (keyIt->second.first) {
            case Task::Upload: {
                /* we have to tell the master that we uploaded this */
                protobuf::RayBagKey& item = *enqueuedProto.add_keys();
                item.set_worker_id(bag.workerId);
                item.set_treelet_id(bag.treeletId);
                item.set_bag_id(bag.bagId);
                item.set_size(bag.size);
                break;
            }

            case Task::Download:
                /* we have to put the received bag on the receive queue */
                receiveQueue.emplace(keyIt->second.second, move(action.data));
                break;
            }

            /* tell the master we've finished uploading this */

            pendingRayBags.erase(keyIt);
        }
    }

    if (enqueuedProto.keys_size() > 0) {
        coordinatorConnection->enqueue_write(
            Message::str(*workerId, OpCode::RayBagEnqueued,
                         protoutil::to_string(enqueuedProto)));
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
            case Action::Upload:
                outgoingRequest =
                    S3PutRequest(config.credentials, config.endpoint,
                                 config.region, action.key, action.data,
                                 UNSIGNED_PAYLOAD)
                        .to_http_request();
                break;

            case Action::Download:
                outgoingRequest =
                    S3GetRequest(config.credentials, config.endpoint,
                                 config.region, action.key)
                        .to_http_request();
                break;
            }

            responses.new_request_arrived(outgoingRequest);
            s3.write(outgoingRequest.str());

            size_t responseCount = 0;

            while (responseCount < 1) {
                responses.parse(s3.read());

                if (!responses.empty()) {
                    if (responses.front().first_line() != "HTTP/1.1 200 OK") {
                        throw runtime_error("TransferAgent::doAction failed ");
                    } else {
                        responseCount++;
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
