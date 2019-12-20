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
                queue.back().info.bagSize + RayState::MaxCompressedSize() >
                    MAX_BAG_SIZE) {
                /* let's create an empty bag */
                queue.emplace(*workerId, treeletId, currentBagId[treeletId]++,
                              false, MAX_BAG_SIZE);
            }

            auto& bag = queue.back();
            auto& ray = rayList.front();

            const auto len = ray->Serialize(&bag.data[0] + bag.info.bagSize);
            bag.info.rayCount++;
            bag.info.bagSize += len;

            rayList.pop();
        }
    }

    return ResultType::Continue;
}

ResultType LambdaWorker::handleFinishedRays() {
    auto& out = finishedQueue;

    if (out.empty()) {
        out.emplace(*workerId, 0, currentFinishedBagId++, true, MAX_BAG_SIZE);
    }

    while (!finishedRays.empty()) {
        auto& ray = finishedRays.front();

        if (out.back().info.bagSize + FinishedRay::MaxCompressedSize() >
            MAX_BAG_SIZE) {
            out.emplace(*workerId, 0, currentFinishedBagId++, true,
                        MAX_BAG_SIZE);
        }

        auto& bag = out.back();
        const auto len = ray.Serialize(&bag.data[0] + bag.info.bagSize);
        bag.info.rayCount++;
        bag.info.bagSize += len;

        finishedRays.pop();
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
            auto& bag = queue.front();
            bag.data.erase(bag.info.bagSize);

            const auto id = transferAgent.requestUpload(
                bag.info.str(rayBagsKeyPrefix), move(bag.data));

            pendingRayBags[id] = make_pair(Task::Upload, bag.info);
            queue.pop();
        }
    }

    return ResultType::Continue;
}

ResultType LambdaWorker::handleFinishedQueue() {
    finishedQueueTimer.reset();

    while (!finishedQueue.empty()) {
        RayBag& bag = finishedQueue.front();
        bag.data.erase(bag.info.bagSize);

        const auto id = transferAgent.requestUpload(
            bag.info.str(rayBagsKeyPrefix), move(bag.data));

        pendingRayBags[id] = make_pair(Task::Upload, bag.info);
        finishedQueue.pop();
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

            traceQueue.push(move(ray));
        }
    }

    return ResultType::Continue;
}

ResultType LambdaWorker::handleTransferResults() {
    protobuf::RayBags enqueuedProto;
    protobuf::RayBags dequeuedProto;

    while (!transferAgent.empty()) {
        TransferAgent::Action action = move(transferAgent.pop());

        auto infoIt = pendingRayBags.find(action.id);
        if (infoIt != pendingRayBags.end()) {
            const auto& info = infoIt->second.second;

            switch (infoIt->second.first) {
            case Task::Upload: {
                /* we have to tell the master that we uploaded this */
                *enqueuedProto.add_items() = to_protobuf(info);
                break;
            }

            case Task::Download:
                /* we have to put the received bag on the receive queue,
                   and tell the master */
                receiveQueue.emplace(info, move(action.data));
                *dequeuedProto.add_items() = to_protobuf(info);
                break;
            }

            /* tell the master we've finished uploading this */

            pendingRayBags.erase(infoIt);
        }
    }

    if (enqueuedProto.items_size() > 0) {
        coordinatorConnection->enqueue_write(
            Message::str(*workerId, OpCode::RayBagEnqueued,
                         protoutil::to_string(enqueuedProto)));
    }

    if (dequeuedProto.items_size() > 0) {
        coordinatorConnection->enqueue_write(
            Message::str(*workerId, OpCode::RayBagDequeued,
                         protoutil::to_string(dequeuedProto)));
    }

    return ResultType::Continue;
}
