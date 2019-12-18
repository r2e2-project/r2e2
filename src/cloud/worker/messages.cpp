#include "cloud/lambda-worker.h"

#include "messages/utils.h"

using namespace std;
using namespace meow;
using namespace std::chrono;
using namespace pbrt;
using namespace PollerShortNames;

using OpCode = Message::OpCode;
using PollerResult = Poller::Result::Type;

Message LambdaWorker::createConnectionRequest(const Worker& peer) {
    protobuf::ConnectRequest proto;
    proto.set_worker_id(*workerId);
    proto.set_my_seed(mySeed);
    proto.set_your_seed(peer.seed);
    return {*workerId, OpCode::ConnectionRequest, protoutil::to_string(proto)};
}

Message LambdaWorker::createConnectionResponse(const Worker& peer) {
    protobuf::ConnectResponse proto;
    proto.set_worker_id(*workerId);
    proto.set_my_seed(mySeed);
    proto.set_your_seed(peer.seed);
    for (const auto& treeletId : treeletIds) {
        proto.add_treelet_ids(treeletId);
    }
    return {*workerId, OpCode::ConnectionResponse, protoutil::to_string(proto)};
}

ResultType LambdaWorker::handlePeers() {
    RECORD_INTERVAL("handlePeers");
    peerTimer.reset();

    const auto now = packet_clock::now();

    for (auto it = peers.begin(); it != peers.end();) {
        auto& peerId = it->first;
        auto& peer = it->second;

        switch (peer.state) {
        case Worker::State::Connecting: {
            auto message = createConnectionRequest(peer);
            servicePackets.emplace_front(peer.address, peer.id, message.str());
            peer.tries++;
            break;
        }

        case Worker::State::Connected:
            break;
        }

        it++;
    }

    return ResultType::Continue;
}

ResultType LambdaWorker::handleNeededTreelets() {
    RECORD_INTERVAL("handleNeededTreelets");
    for (const auto& treeletId : neededTreelets) {
        if (requestedTreelets.count(treeletId)) {
            continue;
        }

        protobuf::GetWorker proto;
        proto.set_treelet_id(treeletId);
        Message message(*workerId, OpCode::GetWorker,
                        protoutil::to_string(proto));
        coordinatorConnection->enqueue_write(message.str());
        requestedTreelets.insert(treeletId);
    }

    neededTreelets.clear();
    return ResultType::Continue;
}

ResultType LambdaWorker::handleMessages() {
    RECORD_INTERVAL("handleMessages");
    MessageParser unprocessedMessages;
    while (!messageParser.empty()) {
        Message message = move(messageParser.front());
        messageParser.pop();

        if (!processMessage(message)) {
            unprocessedMessages.push(move(message));
        }
    }

    swap(messageParser, unprocessedMessages);

    return ResultType::Continue;
}

ResultType LambdaWorker::handleReconnects() {
    reconnectTimer.reset();

    for (const auto dst : reconnectRequests) {
        coordinatorConnection->enqueue_write(
            Message::str(*workerId, OpCode::Reconnect, to_string(dst)));

        peers.at(dst).reset();
    }

    reconnectRequests.clear();

    return ResultType::Continue;
}

bool LambdaWorker::processMessage(const Message& message) {
    /* cerr << "[msg:" << Message::OPCODE_NAMES[to_underlying(message.opcode())]
         << "]" << endl; */

    auto handleConnectTo = [this](const protobuf::ConnectTo& proto) {
        if (peers.count(proto.worker_id()) == 0 &&
            proto.worker_id() != *workerId) {
            const auto dest = Address::decompose(proto.address());
            peers.emplace(proto.worker_id(),
                          Worker{proto.worker_id(), {dest.first, dest.second}});

            addressToWorker.emplace(Address{dest.first, dest.second},
                                    proto.worker_id());
        }
    };

    switch (message.opcode()) {
    case OpCode::Hey: {
        protobuf::Hey proto;
        protoutil::from_string(message.payload(), proto);
        workerId.reset(proto.worker_id());
        jobId.reset(proto.job_id());

        logPrefix = "logs/" + (*jobId) + "/";
        outputName = to_string(*workerId) + ".rays";

        cerr << "worker-id=" << *workerId << endl;

        /* send connection request */
        Address addrCopy{coordinatorAddr};
        peers.emplace(0, Worker{0, move(addrCopy)});
        Message message = createConnectionRequest(peers.at(0));
        servicePackets.emplace_front(coordinatorAddr, 0, message.str());
        break;
    }

    case OpCode::Ping: {
        /* Message pong{OpCode::Pong, ""};
        coordinatorConnection->enqueue_write(pong.str()); */
        break;
    }

    case OpCode::GetObjects: {
        protobuf::GetObjects proto;
        protoutil::from_string(message.payload(), proto);
        getObjects(proto);
        initializeScene();
        break;
    }

    case OpCode::GenerateRays: {
        RECORD_INTERVAL("generateRays");
        protobuf::GenerateRays proto;
        protoutil::from_string(message.payload(), proto);
        generateRays(from_protobuf(proto.crop_window()));
        break;
    }

    case OpCode::ConnectTo: {
        protobuf::ConnectTo proto;
        protoutil::from_string(message.payload(), proto);
        handleConnectTo(proto);
        break;
    }

    case OpCode::Reconnect: {
        const auto target = stoull(message.payload());

        if (peers.count(target)) {
            peers.at(target).reset();
        }

        break;
    }

    case OpCode::MultipleConnect: {
        protobuf::ConnectTo proto;
        protobuf::RecordReader reader{istringstream{message.payload()}};

        while (!reader.eof()) {
            reader.read(&proto);
            handleConnectTo(proto);
        }

        break;
    }

    case OpCode::ConnectionRequest: {
        protobuf::ConnectRequest proto;
        protoutil::from_string(message.payload(), proto);

        const auto otherWorkerId = proto.worker_id();
        if (peers.count(otherWorkerId) == 0) {
            /* we haven't heard about this peer from the master, let's process
             * it later */
            return false;
        }

        auto& peer = peers.at(otherWorkerId);
        auto message = createConnectionResponse(peer);
        servicePackets.emplace_front(peer.address, otherWorkerId,
                                     message.str());
        break;
    }

    case OpCode::ConnectionResponse: {
        protobuf::ConnectResponse proto;
        protoutil::from_string(message.payload(), proto);

        const auto otherWorkerId = proto.worker_id();
        if (peers.count(otherWorkerId) == 0) {
            /* we don't know about this worker */
            return true;
        }

        auto& peer = peers.at(otherWorkerId);
        peer.seed = proto.my_seed();
        if (peer.state != Worker::State::Connected &&
            proto.your_seed() == mySeed) {
            peer.state = Worker::State::Connected;
            peer.nextKeepAlive = packet_clock::now() + KEEP_ALIVE_INTERVAL;

            for (const auto treeletId : proto.treelet_ids()) {
                peer.treelets.insert(treeletId);
                treeletToWorker[treeletId].push_back(otherWorkerId);
                neededTreelets.erase(treeletId);
                requestedTreelets.erase(treeletId);

                if (pendingQueue.count(treeletId)) {
                    auto& treeletPending = pendingQueue[treeletId];
                    auto& treeletOut = outQueue[treeletId];

                    outQueueSize += treeletPending.size();
                    pendingQueueSize -= treeletPending.size();

                    while (!treeletPending.empty()) {
                        auto& front = treeletPending.front();
                        workerStats.recordSendingRay(*front);
                        treeletOut.push_back(move(front));
                        treeletPending.pop_front();
                    }
                }
            }
        }

        break;
    }

    case OpCode::SendRays: {
        char const* data = message.payload().data();
        const uint64_t queueLen = *reinterpret_cast<uint64_t const*>(data);
        data += sizeof(queueLen);

        const uint32_t dataLen = message.payload().length() - sizeof(queueLen);
        uint32_t offset = 0;
        bool first = true;

        while (offset < dataLen) {
            const auto len = *reinterpret_cast<const uint32_t*>(data + offset);
            offset += 4;

            RayStatePtr ray = RayState::Create();
            ray->Deserialize(data + offset, len);
            ray->hop++;
            offset += len;

            if (first) {
                workerStats.recordReceivedBytes(ray->CurrentTreelet(), dataLen);
                first = false;
            }

            logRayAction(*ray, RayAction::Received, message.sender_id());
            pushTraceQueue(move(ray));
        }

        break;
    }

    case OpCode::Bye:
        terminate();
        break;

    case OpCode::StartBenchmark: {
        Chunk c{message.payload()};
        const uint32_t destination = c.be32();
        const uint32_t duration = c(4).be32();
        const uint32_t rate = c(8).be32();
        initBenchmark(duration, destination, rate);
        break;
    }

    default:
        throw runtime_error("unhandled message opcode");
    }

    return true;
}
