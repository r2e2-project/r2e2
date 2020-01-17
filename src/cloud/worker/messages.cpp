#include "cloud/lambda-worker.h"

#include "messages/utils.h"

using namespace std;
using namespace meow;
using namespace std::chrono;
using namespace pbrt;
using namespace PollerShortNames;

using OpCode = Message::OpCode;
using PollerResult = Poller::Result::Type;

constexpr char LOG_STREAM_ENVAR[] = "AWS_LAMBDA_LOG_STREAM_NAME";

ResultType LambdaWorker::handleMessages() {
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
        rayBagsKeyPrefix = "jobs/" + (*jobId) + "/";

        cerr << protoutil::to_json(proto) << endl;

        coordinatorConnection->enqueue_write(
            Message::str(0, OpCode::Hey, safe_getenv_or(LOG_STREAM_ENVAR, "")));

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
        protobuf::GenerateRays proto;
        protoutil::from_string(message.payload(), proto);
        generateRays(from_protobuf(proto.crop_window()));
        break;
    }

    case OpCode::ProcessRayBag: {
        protobuf::RayBags proto;
        protoutil::from_string(message.payload(), proto);

        for (const protobuf::RayBagInfo& item : proto.items()) {
            RayBagInfo info{from_protobuf(item)};
            const auto id =
                transferAgent.requestDownload(info.str(rayBagsKeyPrefix));
            pendingRayBags[id] = make_pair(Task::Download, info);

            logBag(BagAction::Requested, info);
        }

        break;
    }

    case OpCode::FinishUp:
        loop.poller().add_action(Poller::Action(
            alwaysOnFd, Direction::Out,
            [this]() {
                sendWorkerStats();

                coordinatorConnection->enqueue_write(
                    Message::str(*workerId, OpCode::Bye, ""));

                return ResultType::Cancel;
            },
            [this]() {
                return traceQueue.empty() && outQueue.empty() &&
                       samples.empty() && openBags.empty() &&
                       sealedBags.empty() && receiveQueue.empty() &&
                       pendingRayBags.empty() && sampleBags.empty();
            },
            []() { throw runtime_error("terminating failed"); }));

        break;

    case OpCode::Bye:
        terminate();
        break;

    default:
        throw runtime_error("unhandled message opcode");
    }
}
