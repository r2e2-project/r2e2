#include "cloud/lambda-master.h"

#include <chrono>

#include "cloud/r2t2.h"
#include "execution/meow/message.h"
#include "messages/utils.h"

using namespace std;
using namespace std::chrono;
using namespace pbrt;
using namespace meow;
using namespace PollerShortNames;

using OpCode = Message::OpCode;

ResultType LambdaMaster::handleMessages() {
    while (!incomingMessages.empty()) {
        auto front = move(incomingMessages.front());
        incomingMessages.pop_front();
        processMessage(front.first, front.second);
    }

    return ResultType::Continue;
}

void LambdaMaster::processMessage(const uint64_t workerId,
                                  const meow::Message &message) {
    /* cerr << "[msg:" << Message::OPCODE_NAMES[to_underlying(message.opcode())]
         << "] from worker " << workerId << endl; */

    auto &worker = workers.at(workerId);

    switch (message.opcode()) {
    case OpCode::Hey: {
        worker.awsLogStream = message.payload();

        protobuf::Hey heyProto;
        heyProto.set_worker_id(workerId);
        heyProto.set_job_id(jobId);
        Message msg{0, OpCode::Hey, protoutil::to_string(heyProto)};
        worker.connection->enqueue_write(msg.str());

        if (!worker.initialized) {
            worker.initialized = true;
            initializedWorkers++;
        }

        break;
    }

    case OpCode::WorkerStats: {
        protobuf::WorkerStats proto;
        protoutil::from_string(message.payload(), proto);
        auto stats = from_protobuf(proto);

        if (stats.finishedRays() != 0) {
            lastFinishedRay = lastActionTime = steady_clock::now();
        }

        /* merge into global worker stats */
        workerStats.merge(stats);

        /* merge into local worker stats */
        auto &worker = workers.at(workerId);
        worker.stats.merge(stats);

        if (config.workerStatsWriteInterval > 0 &&
            worker.nextStatusLogTimestamp < proto.timestamp_us()) {
            if (worker.nextStatusLogTimestamp == 0) {
                statsOstream << "start " << worker.id << ' '
                             << proto.worker_start_us() << '\n';
            }

            statsOstream << worker.id << ' ' << proto.timestamp_us() << ' '
                         << protoutil::to_json(to_protobuf(worker.stats))
                         << '\n';

            worker.nextStatusLogTimestamp =
                duration_cast<microseconds>(workerStatsWriteInterval).count() +
                proto.timestamp_us();
        }

        /* if (tiles.canSendTiles && tiles.cameraRaysRemaining() &&
            stats.queueStats.pending + stats.queueStats.out +
                    stats.queueStats.ray <
                config.newTileThreshold) {
            tiles.sendWorkerTile(worker);
        } */

        break;
    }

    case OpCode::FinishedRays: {
        protobuf::RecordReader finishedReader{istringstream(message.payload())};
        vector<FinishedRay> finishedRays;

        while (!finishedReader.eof()) {
            protobuf::FinishedRay proto;
            if (finishedReader.read(&proto)) {
                finishedRays.push_back(from_protobuf(proto));
            }
        }

        graphics::AccumulateImage(scene.camera, finishedRays);
        break;
    }

    case OpCode::FinishedPaths: {
        Chunk chunk{message.payload()};

        while (chunk.size()) {
            finishedPathIds.insert(chunk.be64());
            chunk = chunk(8);
        }

        break;
    }

    case OpCode::RayBagEnqueued: {
        protobuf::RayBagKeys proto;
        protoutil::from_string(message.payload(), proto);

        for (const auto &item : proto.keys()) {
            const RayBagKey key = from_protobuf(item);

            if (objectManager.assignedTreelets.count(key.treeletId)) {
                queuedRayBags[key.treeletId].push(key);
            } else {
                pendingRayBags[key.treeletId].push(key);
            }

            queueSize[key.treeletId] += key.size;
        }

        break;
    }

    case OpCode::RayBagDequeued: {
        protobuf::RayBagKeys proto;
        protoutil::from_string(message.payload(), proto);

        for (const auto &item : proto.keys()) {
            const RayBagKey key = from_protobuf(item);
            queueSize[key.treeletId] -= key.size;
        }
    }

    default:
        throw runtime_error("unhandled message opcode: " +
                            to_string(to_underlying(message.opcode())));
    }
}
