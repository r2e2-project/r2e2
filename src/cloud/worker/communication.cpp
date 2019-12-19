#include "cloud/lambda-worker.h"

#include "messages/utils.h"

using namespace std;
using namespace pbrt;
using namespace meow;
using namespace PollerShortNames;

using OpCode = Message::OpCode;

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
    protobuf::RayBagKeys dequeuedProto;

    while (!transferAgent.empty()) {
        TransferAgent::Action action = move(transferAgent.pop());

        auto keyIt = pendingRayBags.find(action.id);
        if (keyIt != pendingRayBags.end()) {
            const auto& key = keyIt->second.second;

            switch (keyIt->second.first) {
            case Task::Upload: {
                /* we have to tell the master that we uploaded this */
                *enqueuedProto.add_keys() = to_protobuf(key);
                break;
            }

            case Task::Download:
                /* we have to put the received bag on the receive queue,
                   and tell the master */
                receiveQueue.emplace(key, move(action.data));
                *dequeuedProto.add_keys() = to_protobuf(key);
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

    if (dequeuedProto.keys_size() > 0) {
        coordinatorConnection->enqueue_write(
            Message::str(*workerId, OpCode::RayBagDequeued,
                         protoutil::to_string(dequeuedProto)));
    }

    return ResultType::Continue;
}
