#include "cloud/lambda-master.h"

#include <chrono>
#include <typeinfo>

#include "cloud/r2t2.h"
#include "execution/meow/message.h"
#include "messages/utils.h"

using namespace std;
using namespace std::chrono;
using namespace pbrt;
using namespace meow;
using namespace PollerShortNames;

using OpCode = Message::OpCode;

/* if (tiles.canSendTiles && tiles.cameraRaysRemaining() &&
    stats.queueStats.pending + stats.queueStats.out +
            stats.queueStats.ray <
        config.newTileThreshold) {
    tiles.sendWorkerTile(worker);
} */

ResultType LambdaMaster::handleMessages() {
    while (!incomingMessages.empty()) {
        auto &front = incomingMessages.front();
        processMessage(front.first, front.second);
        incomingMessages.pop_front();
    }

    return ResultType::Continue;
}

void LambdaMaster::processMessage(const uint64_t workerId,
                                  const meow::Message &message) {
    /* cerr << "[msg:" << Message::OPCODE_NAMES[to_underlying(message.opcode())]
         << "] from worker " << workerId << endl; */

    lastActionTime = steady_clock::now();

    auto &worker = workers.at(workerId);
    worker.lastSeen = lastActionTime;

    switch (message.opcode()) {
    case OpCode::Hey: {
        worker.awsLogStream = message.payload();

        protobuf::Hey proto;
        proto.set_worker_id(workerId);
        proto.set_job_id(jobId);
        worker.connection->enqueue_write(
            Message::str(0, OpCode::Hey, protoutil::to_string(proto)));

        if (!worker.initialized) {
            worker.initialized = true;
            initializedWorkers++;
        }

        break;
    }

    case OpCode::RayBagEnqueued: {
        protobuf::RayBags proto;
        protoutil::from_string(message.payload(), proto);

        for (const auto &item : proto.items()) {
            const RayBagInfo info = from_protobuf(item);
            logEnqueue(workerId, info);

            if (info.sampleBag) {
                continue;
            }

            if (objectManager.unassignedTreelets.count(info.treeletId) == 0) {
                queuedRayBags[info.treeletId].push(info);
            } else {
                pendingRayBags[info.treeletId].push(info);
            }
        }

        break;
    }

    case OpCode::RayBagDequeued: {
        protobuf::RayBags proto;
        protoutil::from_string(message.payload(), proto);

        for (const auto &item : proto.items()) {
            const RayBagInfo info = from_protobuf(item);
            logDequeue(workerId, info);
        }

        break;
    }

    case OpCode::WorkerStats: {
        protobuf::WorkerStats proto;
        protoutil::from_string(message.payload(), proto);

        aggregatedStats.finishedPaths += proto.finished_paths();

        break;
    }

    default:
        throw runtime_error("unhandled message opcode: " +
                            to_string(to_underlying(message.opcode())));
    }
}
