#include "cloud/lambda-worker.h"

#include "messages/utils.h"

using namespace std;
using namespace meow;
using namespace std::chrono;
using namespace pbrt;
using namespace PollerShortNames;

using OpCode = Message::OpCode;
using PollerResult = Poller::Result::Type;

ResultType LambdaWorker::handleMessages() {
    RECORD_INTERVAL("handleMessages");

    while (!messageParser.empty()) {
        processMessage(messageParser.front());
        messageParser.pop();
    }

    return ResultType::Continue;
}

void LambdaWorker::processMessage(const Message& message) {
    /* cerr << "[msg:" << Message::OPCODE_NAMES[to_underlying(message.opcode())]
         << "]" << endl; */

    switch (message.opcode()) {
    case OpCode::Hey: {
        protobuf::Hey proto;
        protoutil::from_string(message.payload(), proto);
        workerId.reset(proto.worker_id());
        jobId.reset(proto.job_id());

        logPrefix = "logs/" + (*jobId) + "/";
        rayBagsKeyPrefix = "jobs/" + (*jobId) + "/rays/";

        cerr << "worker-id=" << *workerId << endl;
        break;
    }

    case OpCode::Ping: {
        break;
    }

    case OpCode::GetObjects: {
        protobuf::GetObjects proto;
        protoutil::from_string(message.payload(), proto);
        getObjects(proto);
        scene.initialize();
        break;
    }

    case OpCode::GenerateRays: {
        RECORD_INTERVAL("generateRays");
        protobuf::GenerateRays proto;
        protoutil::from_string(message.payload(), proto);
        generateRays(from_protobuf(proto.crop_window()));
        break;
    }

    case OpCode::ProcessRayBag: {
        protobuf::RayBagKeys proto;

        for (const protobuf::RayBagKey& item : proto.keys()) {
            RayBagKey key{from_protobuf(item)};
            const auto id =
                transferAgent.requestDownload(key.str(rayBagsKeyPrefix));

            pendingRayBags[id] = make_pair(Task::Download, key);
        }

        break;
    }

    case OpCode::Bye:
        terminate();
        break;

    default:
        throw runtime_error("unhandled message opcode");
    }
}
