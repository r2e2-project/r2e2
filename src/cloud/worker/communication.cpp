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
            auto& ray = rayList.front();

            if (queue.empty() ||
                queue.back().info.bagSize + ray->MaxCompressedSize() >
                    MAX_BAG_SIZE) {
                /* let's create an empty bag */
                queue.emplace(*workerId, treeletId, currentBagId[treeletId]++,
                              false, MAX_BAG_SIZE);
            }

            auto& bag = queue.back();

            const auto len = ray->Serialize(&bag.data[0] + bag.info.bagSize);
            bag.info.rayCount++;
            bag.info.bagSize += len;

            rayList.pop();
            outQueueSize--;
        }
    }

    return ResultType::Continue;
}

ResultType LambdaWorker::handleSamples() {
    auto& out = sampleBags;

    if (out.empty()) {
        out.emplace(*workerId, 0, currentSampleBagId++, true, MAX_BAG_SIZE);
    }

    while (!samples.empty()) {
        auto& sample = samples.front();

        if (out.back().info.bagSize + sample.MaxCompressedSize() >
            MAX_BAG_SIZE) {
            out.emplace(*workerId, 0, currentSampleBagId++, true, MAX_BAG_SIZE);
        }

        auto& bag = out.back();
        const auto len = sample.Serialize(&bag.data[0] + bag.info.bagSize);
        bag.info.rayCount++;
        bag.info.bagSize += len;

        samples.pop();
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

ResultType LambdaWorker::handleSampleBags() {
    sampleBagsTimer.reset();

    while (!sampleBags.empty()) {
        RayBag& bag = sampleBags.front();
        bag.data.erase(bag.info.bagSize);

        const auto id = transferAgent.requestUpload(
            bag.info.str(rayBagsKeyPrefix), move(bag.data));

        pendingRayBags[id] = make_pair(Task::Upload, bag.info);
        sampleBags.pop();
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

    if (!transferAgent.eventfd().read_event()) {
        return ResultType::Continue;
    }

    pair<uint64_t, string> action;
    while (transferAgent.try_pop(action)) {
        auto infoIt = pendingRayBags.find(action.first);

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
                receiveQueue.emplace(info, move(action.second));
                *dequeuedProto.add_items() = to_protobuf(info);
                break;
            }

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
