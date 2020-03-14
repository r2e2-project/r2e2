#include <chrono>
#include <typeinfo>

#include "execution/meow/message.h"
#include "lambda-master.h"
#include "messages/utils.h"

using namespace std;
using namespace std::chrono;
using namespace r2t2;
using namespace meow;
using namespace PollerShortNames;

using OpCode = Message::OpCode;

ResultType LambdaMaster::handleMessages() {
    ScopeTimer<TimeLog::Category::HandleMessages> _timer;

    while (!incomingMessages.empty()) {
        auto &front = incomingMessages.front();
        processMessage(front.first, front.second);
        incomingMessages.pop_front();
    }

    return ResultType::Continue;
}

void LambdaMaster::processMessage(const uint64_t workerId,
                                  const Message &message) {
    /* cerr << "[msg:" << Message::OPCODE_NAMES[to_underlying(message.opcode())]
         << "] from worker " << workerId << endl; */

    lastActionTime = steady_clock::now();

    auto &worker = workers.at(workerId);
    worker.lastSeen = lastActionTime;

    switch (message.opcode()) {
    case OpCode::Hey: {
        worker.awsLogStream = message.payload();
        break;
    }

    case OpCode::GetObjects:
        initializedWorkers++;
        break;

    case OpCode::RayBagEnqueued: {
        protobuf::RayBags proto;
        protoutil::from_string(message.payload(), proto);

        worker.rays.generated = proto.rays_generated();
        worker.rays.terminated = proto.rays_terminated();

        for (const auto &item : proto.items()) {
            const RayBagInfo info = from_protobuf(item);
            recordEnqueue(workerId, info);

            if (info.sampleBag) {
                sampleBags.push_back(info);
                continue;
            }

            if (unassignedTreelets.count(info.treeletId) == 0) {
                queuedRayBags[info.treeletId].push(info);
                queuedRayBagsCount++;
            } else {
                pendingRayBags[info.treeletId].push(info);
            }
        }

        if (worker.role == Worker::Role::Generator) {
            if (tiles.cameraRaysRemaining()) {
                /* Tell the worker to generate rays */
                tiles.sendWorkerTile(worker);
            } else if (worker.activeRays() == 0) {
                /* Generator is done, tell worker to finish up */
                worker.connection->enqueue_write(
                    Message::str(0, OpCode::FinishUp, ""));

                worker.state = Worker::State::FinishingUp;
            }
        } else if (worker.activeRays() < WORKER_MAX_ACTIVE_RAYS) {
            freeWorkers.push_back(workerId);
        }

        break;
    }

    case OpCode::RayBagDequeued: {
        protobuf::RayBags proto;
        protoutil::from_string(message.payload(), proto);

        worker.rays.generated = proto.rays_generated();
        worker.rays.terminated = proto.rays_terminated();

        for (const auto &item : proto.items()) {
            const RayBagInfo info = from_protobuf(item);
            recordDequeue(workerId, info);
        }

        break;
    }

    case OpCode::WorkerStats: {
        protobuf::WorkerStats proto;
        protoutil::from_string(message.payload(), proto);

        WorkerStats stats = from_protobuf(proto);

        worker.stats.finishedPaths += stats.finishedPaths;
        worker.stats.cpuUsage = stats.cpuUsage;

        aggregatedStats.finishedPaths += stats.finishedPaths;

        break;
    }

    case OpCode::Bye: {
        if (worker.state == Worker::State::FinishingUp) {
            /* it's fine for this worker to say bye */
            worker.state = Worker::State::Terminating;
        }

        worker.connection->enqueue_write(Message::str(0, OpCode::Bye, ""));
        break;
    }

    default:
        throw runtime_error("unhandled message opcode: " +
                            to_string(to_underlying(message.opcode())));
    }
}
