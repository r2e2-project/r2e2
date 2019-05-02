#include "lambda-worker.h"

#include <getopt.h>
#include <glog/logging.h>
#include <stdlib.h>
#include <sys/resource.h>
#include <sys/timerfd.h>
#include <cstdlib>
#include <iterator>
#include <limits>
#include <sstream>
#include <stdexcept>

#include "cloud/bvh.h"
#include "cloud/integrator.h"
#include "cloud/lambda-master.h"
#include "cloud/manager.h"
#include "cloud/raystate.h"
#include "cloud/stats.h"
#include "core/camera.h"
#include "core/geometry.h"
#include "core/light.h"
#include "core/sampler.h"
#include "core/spectrum.h"
#include "core/transform.h"
#include "execution/loop.h"
#include "execution/meow/message.h"
#include "messages/utils.h"
#include "net/address.h"
#include "net/requests.h"
#include "net/util.h"
#include "storage/backend.h"
#include "util/exception.h"
#include "util/path.h"
#include "util/random.h"
#include "util/system_runner.h"
#include "util/temp_dir.h"
#include "util/temp_file.h"

using namespace std;
using namespace chrono;
using namespace meow;
using namespace pbrt;
using namespace pbrt::global;
using namespace PollerShortNames;

using OpCode = Message::OpCode;
using PollerResult = Poller::Result::Type;

constexpr size_t UDP_MTU_BYTES{1'400};
constexpr milliseconds PEER_CHECK_INTERVAL{250};
constexpr milliseconds HANDLE_ACKS_INTERVAL{50};
constexpr milliseconds WORKER_STATS_INTERVAL{1'000};
constexpr milliseconds WORKER_DIAGNOSTICS_INTERVAL{2'000};
constexpr milliseconds KEEP_ALIVE_INTERVAL{40'000};
constexpr milliseconds FINISHED_PATHS_INTERVAL{2'500};
constexpr milliseconds PACKET_TIMEOUT{2'000};
constexpr char LOG_STREAM_ENVAR[] = "AWS_LAMBDA_LOG_STREAM_NAME";

LambdaWorker::LambdaWorker(const string& coordinatorIP,
                           const uint16_t coordinatorPort,
                           const string& storageUri, const bool sendReliably,
                           const int samplesPerPixel,
                           const FinishedRayAction finishedRayAction,
                           const float rayActionsLogRate)
    : rayActionsLogRate(rayActionsLogRate),
      sendReliably(sendReliably),
      coordinatorAddr(coordinatorIP, coordinatorPort),
      workingDirectory("/tmp/pbrt-worker"),
      storageBackend(StorageBackend::create_backend(storageUri)),
      finishedRayAction(finishedRayAction),
      samplesPerPixel(samplesPerPixel),
      peerTimer(PEER_CHECK_INTERVAL),
      workerStatsTimer(WORKER_STATS_INTERVAL),
      workerDiagnosticsTimer(WORKER_DIAGNOSTICS_INTERVAL),
      finishedPathsTimer(FINISHED_PATHS_INTERVAL),
      handleRayAcknowledgementsTimer(HANDLE_ACKS_INTERVAL) {
    cerr << "* starting worker in " << workingDirectory.name() << endl;

    LOG(INFO) << "WORKER UP";

    roost::chdir(workingDirectory.name());

    FLAGS_log_dir = ".";
    google::InitGoogleLogging(logBase.c_str());
    diagnosticsOstream.open(diagnosticsName, ios::out | ios::trunc);

    diagnosticsOstream << "start "
                       << duration_cast<microseconds>(
                              workerDiagnostics.startTime.time_since_epoch())
                              .count()
                       << endl;

    if (trackRays) {
        rayActionsOstream.open(rayActionsName, ios::out | ios::trunc);
        rayActionsOstream << "x,y,sample,tick,hop,shadowRay,workerID,otherPartyID,"
                             "treeletID,timestamp,action"
                          << endl;
    }

    PbrtOptions.nThreads = 1;
    bvh = make_shared<CloudBVH>();
    manager.init(".");

    srand(time(nullptr));
    do {
        mySeed = rand();
    } while (mySeed == 0);

    coordinatorConnection = loop.make_connection<TCPConnection>(
        coordinatorAddr,
        [this](shared_ptr<TCPConnection>, string&& data) {
            RECORD_INTERVAL("parseTCP");
            this->tcpMessageParser.parse(data);

            while (!this->tcpMessageParser.empty()) {
                this->messageParser.push(move(this->tcpMessageParser.front()));
                this->tcpMessageParser.pop();
            }

            return true;
        },
        []() { LOG(INFO) << "Connection to coordinator failed."; },
        [this]() { this->terminate(); });

    loop.poller().add_action(Poller::Action(
        udpConnection.socket(), Direction::In,
        bind(&LambdaWorker::handleUdpReceive, this), [this]() { return true; },
        []() { throw runtime_error("udp in failed"); }));

    loop.poller().add_action(Poller::Action(
        handleRayAcknowledgementsTimer.fd, Direction::In,
        bind(&LambdaWorker::handleRayAcknowledgements, this),
        [this]() {
            return !toBeAcked.empty() ||
                   (!receivedAcks.empty() && !outstandingRayPackets.empty() &&
                    outstandingRayPackets.front().first <= packet_clock::now());
        },
        []() { throw runtime_error("acks failed"); }));

    loop.poller().add_action(Poller::Action(
        udpConnection.socket(), Direction::Out,
        bind(&LambdaWorker::handleUdpSend, this),
        [this]() {
            return (!servicePackets.empty() || !rayPackets.empty()) &&
                   udpConnection.within_pace();
        },
        []() { throw runtime_error("udp out failed"); }));

    /* trace rays */
    loop.poller().add_action(Poller::Action(
        dummyFD, Direction::Out, bind(&LambdaWorker::handleRayQueue, this),
        [this]() { return !rayQueue.empty(); },
        []() { throw runtime_error("ray queue failed"); }));

    /* send processed rays */
    loop.poller().add_action(Poller::Action(
        dummyFD, Direction::Out, bind(&LambdaWorker::handleOutQueue, this),
        [this]() { return outQueueSize > 0; },
        []() { throw runtime_error("out queue failed"); }));

    /* send finished rays */
    /* FIXME we're throwing out finished rays, for now */
    loop.poller().add_action(Poller::Action(
        dummyFD, Direction::Out, bind(&LambdaWorker::handleFinishedQueue, this),
        [this]() {
            // clang-format off
            switch (this->finishedRayAction) {
            case FinishedRayAction::Discard: return finishedQueue.size() > 1000;
            case FinishedRayAction::SendBack: return !finishedQueue.empty();
            default: return false;
            }
            // clang-format on
        },
        []() { throw runtime_error("finished queue failed"); }));

    /* handle peers */
    loop.poller().add_action(Poller::Action(
        peerTimer.fd, Direction::In, bind(&LambdaWorker::handlePeers, this),
        [this]() { return !peers.empty(); },
        []() { throw runtime_error("peers failed"); }));

    /* handle received messages */
    loop.poller().add_action(Poller::Action(
        dummyFD, Direction::Out, bind(&LambdaWorker::handleMessages, this),
        [this]() { return !messageParser.empty(); },
        []() { throw runtime_error("messages failed"); }));

    /* request new peers for neighboring treelets */
    /* loop.poller().add_action(Poller::Action(
        dummyFD, Direction::Out,
        bind(&LambdaWorker::handleNeededTreelets, this),
        [this]() { return !neededTreelets.empty(); },
        []() { throw runtime_error("treelet request failed"); })); */

    /* send updated stats */
    loop.poller().add_action(Poller::Action(
        workerStatsTimer.fd, Direction::In,
        bind(&LambdaWorker::handleWorkerStats, this), [this]() { return true; },
        []() { throw runtime_error("worker stats failed"); }));

    /* send back finished paths */
    loop.poller().add_action(
        Poller::Action(finishedPathsTimer.fd, Direction::In,
                       bind(&LambdaWorker::handleFinishedPaths, this),
                       [this]() { return !finishedPathIds.empty(); },
                       []() { throw runtime_error("finished paths failed"); }));

    /* record diagnostics */
    loop.poller().add_action(Poller::Action(
        workerDiagnosticsTimer.fd, Direction::In,
        bind(&LambdaWorker::handleDiagnostics, this), [this]() { return true; },
        []() { throw runtime_error("handle diagnostics failed"); }));

    coordinatorConnection->enqueue_write(
        Message(OpCode::Hey, safe_getenv_or(LOG_STREAM_ENVAR, "")).str());
}

Message LambdaWorker::createConnectionRequest(const Worker& peer) {
    protobuf::ConnectRequest proto;
    proto.set_worker_id(*workerId);
    proto.set_my_seed(mySeed);
    proto.set_your_seed(peer.seed);
    return {OpCode::ConnectionRequest, protoutil::to_string(proto)};
}

Message LambdaWorker::createConnectionResponse(const Worker& peer) {
    protobuf::ConnectResponse proto;
    proto.set_worker_id(*workerId);
    proto.set_my_seed(mySeed);
    proto.set_your_seed(peer.seed);
    for (const auto& treeletId : treeletIds) {
        proto.add_treelet_ids(treeletId);
    }
    return {OpCode::ConnectionResponse, protoutil::to_string(proto)};
}

void LambdaWorker::logRayAction(const RayState& state, const RayAction action,
                                const WorkerId otherParty) {
    state.tick++;

    if (!trackRays || !state.trackRay) return;

    // clang-format off
    // x,y,sample,tick,hop,shadowRay,workerID,otherPartyID,treeletID,timestamp,action
    rayActionsOstream << state.sample.pixel.x << ','
                      << state.sample.pixel.y << ','
                      << state.sample.num << ','
                      << state.tick << ','
                      << state.hop << ','
                      << state.isShadowRay << ','
                      << *workerId << ','
                      << ((action == RayAction::Sent ||
                           action == RayAction::Received) ? otherParty
                                                          : *workerId) << ','
                      << state.CurrentTreelet() << ','
                      << duration_cast<microseconds>(
                             rays_clock::now().time_since_epoch())
                             .count()
                      << ',';
    // clang-format on

    // clang-format off
    switch (action) {
    case RayAction::Generated: rayActionsOstream << "Generated"; break;
    case RayAction::Traced:    rayActionsOstream << "Traced";    break;
    case RayAction::Queued:    rayActionsOstream << "Queued";    break;
    case RayAction::Pending:   rayActionsOstream << "Pending";   break;
    case RayAction::Sent:      rayActionsOstream << "Sent";      break;
    case RayAction::Received:  rayActionsOstream << "Received";  break;
    case RayAction::Finished:  rayActionsOstream << "Finished";  break;

    default: throw runtime_error("invalid ray action");
    }
    // clang-format on

    rayActionsOstream << endl;
}

ResultType LambdaWorker::handleRayQueue() {
    RECORD_INTERVAL("handleRayQueue");

    auto recordFinishedPath = [this](const uint64_t pathId) {
        this->workerStats.recordFinishedPath();
        this->finishedPathIds.push_back(pathId);
    };

    deque<RayStatePtr> processedRays;

    constexpr size_t MAX_RAYS = 20'000;

    for (size_t i = 0; i < MAX_RAYS && !rayQueue.empty(); i++) {
        RayStatePtr rayPtr = popRayQueue();
        RayState& ray = *rayPtr;

        const uint64_t pathId = ray.PathID();

        logRayAction(ray, RayAction::Traced);

        if (!ray.toVisitEmpty()) {
            const uint32_t rayTreelet = ray.toVisitTop().treelet;
            auto newRayPtr = CloudIntegrator::Trace(move(rayPtr), bvh);
            auto& newRay = *newRayPtr;

            const bool hit = newRay.hit;
            const bool emptyVisit = newRay.toVisitEmpty();

            if (newRay.isShadowRay) {
                if (hit || emptyVisit) {
                    newRay.Ld = hit ? 0.f : newRay.Ld;
                    logRayAction(*newRayPtr, RayAction::Finished);
                    finishedQueue.push_back(move(newRayPtr));
                } else {
                    processedRays.push_back(move(newRayPtr));
                }
            } else if (!emptyVisit || hit) {
                processedRays.push_back(move(newRayPtr));
            } else if (emptyVisit) {
                newRay.Ld = 0.f;
                logRayAction(*newRayPtr, RayAction::Finished);
                finishedQueue.push_back(move(newRayPtr));
                recordFinishedPath(pathId);
            }
        } else if (ray.hit) {
            auto newRays = CloudIntegrator::Shade(move(rayPtr), bvh, lights,
                                                  sampler, arena);

            for (auto& newRay : newRays.first) {
                logRayAction(*newRay, RayAction::Generated);
                processedRays.push_back(move(newRay));
            }

            if (newRays.second) recordFinishedPath(pathId);

            if (newRays.first.empty()) {
                /* rayPtr is not touched if if Shade() returned nothing */
                logRayAction(*rayPtr, RayAction::Finished);
            }
        } else {
            throw runtime_error("invalid ray in ray queue");
        }
    }

    while (!processedRays.empty()) {
        RayStatePtr ray = move(processedRays.front());
        processedRays.pop_front();

        workerStats.recordDemandedRay(*ray);
        const TreeletId nextTreelet = ray->CurrentTreelet();

        if (treeletIds.count(nextTreelet)) {
            pushRayQueue(move(ray));
        } else {
            if (treeletToWorker.count(nextTreelet)) {
                workerStats.recordSendingRay(*ray);
                outQueue[nextTreelet].push_back(move(ray));
                outQueueSize++;
            } else {
                logRayAction(*ray, RayAction::Pending);
                workerStats.recordPendingRay(*ray);
                neededTreelets.insert(nextTreelet);
                pendingQueue[nextTreelet].push_back(move(ray));
                pendingQueueSize++;
            }
        }
    }

    return ResultType::Continue;
}

ResultType LambdaWorker::handleOutQueue() {
    RECORD_INTERVAL("handleOutQueue");

    for (auto& q : outQueue) {
        if (q.second.empty()) continue;

        const auto treeletId = q.first;
        auto& outRays = q.second;

        const auto& candidates = treeletToWorker[treeletId];
        const auto& peer =
            peers.at(*random::sample(candidates.begin(), candidates.end()));

        auto& peerSeqNo = sequenceNumbers[peer.address];

        string unpackedRayStr;
        RayStatePtr unpackedRayPtr;

        while (!outRays.empty() || !unpackedRayStr.empty()) {
            ostringstream oss;
            size_t packetLen = 26;
            size_t rayCount = 0;

            vector<unique_ptr<RayState>> trackedRays;

            {
                protobuf::RecordWriter writer{&oss};
                writer.write(*workerId);

                if (!unpackedRayStr.empty()) {
                    writer.write(unpackedRayStr);

                    if (unpackedRayPtr->trackRay) {
                        trackedRays.push_back(move(unpackedRayPtr));
                    }

                    unpackedRayStr.clear();
                    rayCount++;
                }

                while (packetLen < UDP_MTU_BYTES && !outRays.empty()) {
                    RayStatePtr ray = move(outRays.front());
                    outRays.pop_front();
                    outQueueSize--;

                    string rayStr = RayState::serialize(ray);
                    logRayAction(*ray, RayAction::Queued);

                    const size_t len = rayStr.length() + 4;
                    if (len + packetLen > UDP_MTU_BYTES) {
                        unpackedRayStr.swap(rayStr);
                        unpackedRayPtr = move(ray);
                        break;
                    }

                    if (ray->trackRay) {
                        trackedRays.push_back(move(ray));
                    }

                    packetLen += len;
                    writer.write(rayStr);
                    rayCount++;
                }
            }

            oss.flush();

            RayPacket rayPacket{peer.address,
                                peer.id,
                                treeletId,
                                rayCount,
                                Message::str(OpCode::SendRays, oss.str(),
                                             sendReliably, peerSeqNo),
                                sendReliably,
                                peerSeqNo};

            rayPacket.trackedRays = move(trackedRays);
            rayPackets.emplace_back(move(rayPacket));

            peerSeqNo++;
        }
    }

    return ResultType::Continue;
}

ResultType LambdaWorker::handleFinishedPaths() {
    RECORD_INTERVAL("handleFinishedPaths");
    finishedPathsTimer.reset();

    string payload;
    for (const auto pathId : finishedPathIds) {
        payload += put_field(pathId);
    }

    finishedPathIds.clear();

    coordinatorConnection->enqueue_write(
        Message::str(OpCode::FinishedPaths, payload));

    return ResultType::Continue;
}

ResultType LambdaWorker::handleFinishedQueue() {
    RECORD_INTERVAL("handleFinishedQueue");

    auto createFinishedRay = [](const size_t sampleId, const Point2f& pFilm,
                                const Float weight,
                                const Spectrum L) -> protobuf::FinishedRay {
        protobuf::FinishedRay proto;
        proto.set_sample_id(sampleId);
        *proto.mutable_p_film() = to_protobuf(pFilm);
        proto.set_weight(weight);
        *proto.mutable_l() = to_protobuf(L);
        return proto;
    };

    switch (finishedRayAction) {
    case FinishedRayAction::Discard:
        finishedQueue.clear();
        break;

    case FinishedRayAction::SendBack: {
        ostringstream oss;

        {
            protobuf::RecordWriter writer{&oss};

            while (!finishedQueue.empty()) {
                RayStatePtr rayPtr = move(finishedQueue.front());
                RayState& ray = *rayPtr;
                finishedQueue.pop_front();

                Spectrum L{ray.beta * ray.Ld};

                if (L.HasNaNs() || L.y() < -1e-5 || isinf(L.y())) {
                    L = Spectrum(0.f);
                }

                writer.write(createFinishedRay(ray.sample.id, ray.sample.pFilm,
                                               ray.sample.weight, L));
            }
        }

        oss.flush();
        coordinatorConnection->enqueue_write(
            Message::str(OpCode::FinishedRays, oss.str()));

        break;
    }

    case FinishedRayAction::Upload:
        break;

    default:
        throw runtime_error("invalid finished ray action");
    }

    return ResultType::Continue;
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
            servicePackets.emplace_front(peer.address, message.str());
            peer.tries++;
            break;
        }

        case Worker::State::Connected:
            /* send keep alive */
            if (peerId > 0 && peer.nextKeepAlive < now) {
                peer.nextKeepAlive += KEEP_ALIVE_INTERVAL;
                servicePackets.emplace_back(
                    peer.address,
                    Message::str(OpCode::Ping, put_field(*workerId)));
            }

            break;
        }

        it++;
    }

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

ResultType LambdaWorker::handleNeededTreelets() {
    RECORD_INTERVAL("handleNeededTreelets");
    for (const auto& treeletId : neededTreelets) {
        if (requestedTreelets.count(treeletId)) {
            continue;
        }

        protobuf::GetWorker proto;
        proto.set_treelet_id(treeletId);
        Message message(OpCode::GetWorker, protoutil::to_string(proto));
        coordinatorConnection->enqueue_write(message.str());
        requestedTreelets.insert(treeletId);
    }

    neededTreelets.clear();
    return ResultType::Continue;
}

ResultType LambdaWorker::handleRayAcknowledgements() {
    handleRayAcknowledgementsTimer.reset();

    // sending acknowledgements
    for (auto& receivedKv : toBeAcked) {
        string ack;

        for (size_t i = 0; i < receivedKv.second.size(); i++) {
            ack += put_field(receivedKv.second[i]);

            if (ack.length() >= 1'400 or i == receivedKv.second.size() - 1) {
                Message msg{OpCode::Ack, move(ack)};
                servicePackets.emplace_back(receivedKv.first, move(msg.str()));
                ack = {};
            }
        }
    }

    toBeAcked.clear();

    // retransmit outstanding packets
    const auto now = packet_clock::now();

    while (!receivedAcks.empty() && !outstandingRayPackets.empty() &&
           outstandingRayPackets.front().first <= now) {
        auto& packet = outstandingRayPackets.front().second;
        auto& thisReceivedAcks = receivedAcks[packet.destination];

        if (!thisReceivedAcks.contains(packet.sequenceNumber)) {
            packet.retries++;

            if (false && packet.retries > 1) {
                /* sequenceNumbers[packet.destination]-- */;

                const auto& candidates = treeletToWorker[packet.targetTreelet];
                const auto& peer = peers.at(
                    *random::sample(candidates.begin(), candidates.end()));

                packet.destination = peer.address;
                packet.destinationId = peer.id;
                packet.sequenceNumber = sequenceNumbers[packet.destination]++;
                packet.retries = 0;
            }

            packet.retransmission = true;
            rayPackets.push_back(move(packet));
        }

        outstandingRayPackets.pop_front();
    }

    return ResultType::Continue;
}

ResultType LambdaWorker::handleUdpSend() {
    RECORD_INTERVAL("sendUDP");

    /* we always send service packets first */
    auto sendUdpPacket = [this](const Address& peer, const string& payload) {
        udpConnection.bytes_sent += payload.length();
        udpConnection.socket().sendto(peer, payload);
        udpConnection.record_send(payload.length());
    };

    if (!servicePackets.empty()) {
        auto& datagram = servicePackets.front();
        sendUdpPacket(datagram.first, datagram.second);
        servicePackets.pop_front();
        return ResultType::Continue;
    }

    /* packet to send */
    RayPacket& packet = rayPackets.front();

    /* peer to send the packet to */
    sendUdpPacket(packet.destination, packet.data);

    /* do the necessary logging */
    if (packet.retransmission) {
        workerStats.recordResentRays(packet.targetTreelet, packet.rayCount);
    } else {
        workerStats.recordSentRays(packet.targetTreelet, packet.rayCount);
    }

    for (auto& rayPtr : packet.trackedRays) {
        logRayAction(*rayPtr, RayAction::Sent, packet.destinationId);
    }

    if (packet.reliable) {
        outstandingRayPackets.emplace_back(packet_clock::now() + PACKET_TIMEOUT,
                                           move(packet));
    }

    rayPackets.pop_front();
    return ResultType::Continue;
}

ResultType LambdaWorker::handleUdpReceive() {
    RECORD_INTERVAL("receiveUDP");

    auto datagram = udpConnection.socket().recvfrom();
    auto& data = datagram.second;
    udpConnection.bytes_received += data.length();

    messageParser.parse(data);
    auto& messages = messageParser.completed_messages();

    auto it = messages.end();
    while (it != messages.begin()) {
        it--;

        if (it->is_read()) break;
        it->set_read();

        if (it->reliable()) {
            const auto seqNo = it->sequence_number();
            toBeAcked[datagram.first].push_back(seqNo);
            auto& received = receivedPacketSeqNos[datagram.first];

            if (received.contains(seqNo)) {
                it = messages.erase(it);
                continue;
            } else {
                received.insert(seqNo);
            }
        }

        if (it->opcode() == OpCode::Ack) {
            Chunk chunk(it->payload());
            auto& thisReceivedAcks = receivedAcks[datagram.first];

            while (chunk.size()) {
                thisReceivedAcks.insert(chunk.be64());
                chunk = chunk(8);
            }

            it = messages.erase(it);
        }
    }

    return ResultType::Continue;
}

ResultType LambdaWorker::handleWorkerStats() {
    RECORD_INTERVAL("handleWorkerStats");
    workerStatsTimer.reset();

    auto& qStats = workerStats.queueStats;
    qStats.ray = rayQueue.size();
    qStats.finished = finishedQueue.size();
    qStats.pending = pendingQueueSize;
    qStats.out = outQueueSize;
    qStats.connecting =
        count_if(peers.begin(), peers.end(), [](const auto& peer) {
            return peer.second.state == Worker::State::Connecting;
        });
    qStats.connected = peers.size() - qStats.connecting;
    qStats.outstandingUdp = outstandingRayPackets.size();
    qStats.queuedUdp = rayPackets.size();

    auto proto = to_protobuf(workerStats);

    proto.set_timestamp_us(
        duration_cast<microseconds>(now() - workerStats.startTime).count());

    Message message{OpCode::WorkerStats, protoutil::to_string(proto)};
    coordinatorConnection->enqueue_write(message.str());
    workerStats.reset();
    return ResultType::Continue;
}

ResultType LambdaWorker::handleDiagnostics() {
    RECORD_INTERVAL("handleDiagnostics");
    workerDiagnosticsTimer.reset();

    workerDiagnostics.bytesSent =
        udpConnection.bytes_sent - lastDiagnostics.bytesSent;

    workerDiagnostics.bytesReceived =
        udpConnection.bytes_received - lastDiagnostics.bytesReceived;

    workerDiagnostics.outstandingUdp = rayPackets.size();
    lastDiagnostics.bytesSent = udpConnection.bytes_sent;
    lastDiagnostics.bytesReceived = udpConnection.bytes_received;

    const auto timestamp =
        duration_cast<microseconds>(now() - workerDiagnostics.startTime)
            .count();

    diagnosticsOstream << timestamp << " "
                       << protoutil::to_json(to_protobuf(workerDiagnostics))
                       << endl;

    workerDiagnostics.reset();

    return ResultType::Continue;
}

void LambdaWorker::generateRays(const Bounds2i& bounds) {
    const Bounds2i sampleBounds = camera->film->GetSampleBounds();
    const Vector2i sampleExtent = sampleBounds.Diagonal();
    const uint8_t maxDepth = 5;
    const auto samplesPerPixel = sampler->samplesPerPixel;
    const Float rayScale = 1 / sqrt((Float)samplesPerPixel);

    /* for ray tracking */
    mt19937 randEngine(random_device{}());
    bernoulli_distribution bd{rayActionsLogRate};

    for (size_t sample = 0; sample < sampler->samplesPerPixel; sample++) {
        for (const Point2i pixel : bounds) {
            sampler->StartPixel(pixel);
            if (!InsideExclusive(pixel, sampleBounds)) continue;
            sampler->SetSampleNumber(sample);

            CameraSample cameraSample = sampler->GetCameraSample(pixel);

            RayStatePtr statePtr = make_unique<RayState>();
            RayState& state = *statePtr;

            state.trackRay = trackRays ? bd(randEngine) : false;
            state.sample.id =
                (pixel.x + pixel.y * sampleExtent.x) * samplesPerPixel + sample;
            state.sample.num = sample;
            state.sample.pixel = pixel;
            state.sample.pFilm = cameraSample.pFilm;
            state.sample.weight =
                camera->GenerateRayDifferential(cameraSample, &state.ray);
            state.ray.ScaleDifferentials(rayScale);
            state.remainingBounces = maxDepth;
            state.StartTrace();

            logRayAction(state, RayAction::Generated);
            workerStats.recordDemandedRay(state);

            const auto nextTreelet = state.CurrentTreelet();

            if (treeletIds.count(nextTreelet)) {
                pushRayQueue(move(statePtr));
            } else {
                if (treeletToWorker.count(nextTreelet)) {
                    workerStats.recordSendingRay(state);
                    outQueue[nextTreelet].push_back(move(statePtr));
                    outQueueSize++;
                } else {
                    workerStats.recordPendingRay(state);
                    neededTreelets.insert(nextTreelet);
                    pendingQueue[nextTreelet].push_back(move(statePtr));
                    pendingQueueSize++;
                }
            }
        }
    }
}

void LambdaWorker::getObjects(const protobuf::GetObjects& objects) {
    vector<storage::GetRequest> requests;
    for (const protobuf::ObjectKey& objectKey : objects.object_ids()) {
        const ObjectKey id = from_protobuf(objectKey);
        if (id.type == ObjectType::TriangleMesh) {
            /* triangle meshes are packed into treelets, so ignore */
            continue;
        }
        if (id.type == ObjectType::Treelet) {
            treeletIds.insert(id.id);
        }
        const string filePath = id.to_string();
        requests.emplace_back(filePath, filePath);
    }
    storageBackend->get(requests);
}

void LambdaWorker::pushRayQueue(RayStatePtr&& state) {
    workerStats.recordWaitingRay(*state);
    rayQueue.push_back(move(state));
}

RayStatePtr LambdaWorker::popRayQueue() {
    RayStatePtr state = move(rayQueue.front());
    rayQueue.pop_front();

    workerStats.recordProcessedRay(*state);

    return state;
}

bool LambdaWorker::processMessage(const Message& message) {
    /* cerr << "[msg:" << Message::OPCODE_NAMES[to_underlying(message.opcode())]
         << "]\n"; */

    auto handleConnectTo = [this](const protobuf::ConnectTo& proto) {
        if (peers.count(proto.worker_id()) == 0 &&
            proto.worker_id() != *workerId) {
            const auto dest = Address::decompose(proto.address());
            peers.emplace(proto.worker_id(),
                          Worker{proto.worker_id(), {dest.first, dest.second}});
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
        servicePackets.emplace_front(coordinatorAddr, message.str());
        break;
    }

    case OpCode::Ping: {
        /* Message pong{OpCode::Pong, ""};
        coordinatorConnection->enqueue_write(pong.str()); */
        LOG(INFO) << "PING " << Chunk(message.payload()).be64();
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
        servicePackets.emplace_front(peer.address, message.str());
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
        protobuf::RecordReader reader{istringstream{message.payload()}};

        WorkerId senderId;
        reader.read(&senderId);

        while (!reader.eof()) {
            string rayStr;
            if (reader.read(&rayStr)) {
                RayStatePtr ray = RayState::deserialize(rayStr);
                ray->hop++;

                workerStats.recordReceivedRay(*ray);
                logRayAction(*ray, RayAction::Received, senderId);
                pushRayQueue(move(ray));
            }
        }

        break;
    }

    case OpCode::Bye:
        terminate();
        break;

    default:
        throw runtime_error("unhandled message opcode");
    }

    return true;
}

// Minimum, where negative numbers are regarded as infinitely positive.
int min_neg_infinity(const int a, const int b) {
    if (a < 0) return b;
    if (b < 0) return a;
    return min(a, b);
}

void LambdaWorker::run() {
    while (!terminated) {
        // timeouts treat -1 as positive infinity
        int min_timeout_ms = -1;

        // If this connection is not within pace, it requests a timeout when it
        // would be, so that we can re-poll and schedule it.
        const int64_t millis_ahead_of_pace =
            udpConnection.micros_ahead_of_pace() / 1000;
        const int conn_timeout_ms =
            (millis_ahead_of_pace <= 0) ? -1 : millis_ahead_of_pace;

        min_timeout_ms = min_neg_infinity(min_timeout_ms, conn_timeout_ms);

        auto res = loop.loop_once(min_timeout_ms).result;
        if (res != PollerResult::Success && res != PollerResult::Timeout) break;
    }
}

void LambdaWorker::loadCamera() {
    auto reader = manager.GetReader(ObjectType::Camera);
    protobuf::Camera proto_camera;
    reader->read(&proto_camera);
    camera = camera::from_protobuf(proto_camera, transformCache);
    filmTile = camera->film->GetFilmTile(camera->film->GetSampleBounds());
}

void LambdaWorker::loadSampler() {
    auto reader = manager.GetReader(ObjectType::Sampler);
    protobuf::Sampler proto_sampler;
    reader->read(&proto_sampler);
    sampler = sampler::from_protobuf(proto_sampler, samplesPerPixel);

    /* if (workerId.initialized()) {
        sampler = sampler->Clone(*workerId);
    } */
}

void LambdaWorker::loadLights() {
    auto reader = manager.GetReader(ObjectType::Lights);
    while (!reader->eof()) {
        protobuf::Light proto_light;
        reader->read(&proto_light);
        lights.push_back(move(light::from_protobuf(proto_light)));
    }
}

void LambdaWorker::loadFakeScene() {
    auto reader = manager.GetReader(ObjectType::Scene);
    protobuf::Scene proto_scene;
    reader->read(&proto_scene);
    fakeScene = make_unique<Scene>(from_protobuf(proto_scene));
}

void LambdaWorker::initializeScene() {
    if (initialized) return;

    loadCamera();
    loadSampler();
    loadLights();
    loadFakeScene();

    for (auto& light : lights) {
        light->Preprocess(*fakeScene);
    }

    initialized = true;
}

void LambdaWorker::uploadLogs() {
    if (!workerId.initialized()) return;

    google::FlushLogFiles(google::INFO);
    diagnosticsOstream.close();
    rayActionsOstream.close();

    vector<storage::PutRequest> putLogsRequest = {
        {infoLogName, logPrefix + to_string(*workerId) + ".INFO"},
        {diagnosticsName, logPrefix + to_string(*workerId) + ".DIAG"}};

    if (trackRays) {
        putLogsRequest.emplace_back(rayActionsName,
                                    logPrefix + to_string(*workerId) + ".RAYS");
    }

    storageBackend->put(putLogsRequest);
}

void usage(const char* argv0, int exitCode) {
    cerr << "Usage: " << argv0 << " [OPTIONS]" << endl
         << endl
         << "Options:" << endl
         << "  -i --ip IPSTRING           ip of coordinator" << endl
         << "  -p --port PORT             port of coordinator" << endl
         << "  -s --storage-backend NAME  storage backend URI" << endl
         << "  -R --reliable-udp          send ray packets reliably" << endl
         << "  -S --samples N             number of samples per pixel" << endl
         << "  -L --log-rays RATE         log ray actions" << endl
         << "  -f --finished-ray ACTION   what to do with finished rays" << endl
         << "                             * 0: discard (default)" << endl
         << "                             * 1: send" << endl
         << "                             * 2: upload" << endl
         << "  -h --help                  show help information" << endl;
}

int main(int argc, char* argv[]) {
    int exit_status = EXIT_SUCCESS;

    uint16_t listenPort = 50000;
    string publicIp;
    string storageUri;
    bool sendReliably = false;
    int samplesPerPixel = 0;
    FinishedRayAction finishedRayAction = FinishedRayAction::Discard;
    float rayActionsLogRate = 0.0;

    struct option long_options[] = {
        {"port", required_argument, nullptr, 'p'},
        {"ip", required_argument, nullptr, 'i'},
        {"storage-backend", required_argument, nullptr, 's'},
        {"reliable-udp", no_argument, nullptr, 'R'},
        {"samples", required_argument, nullptr, 'S'},
        {"log-rays", required_argument, nullptr, 'L'},
        {"finished-ray", required_argument, nullptr, 'f'},
        {"help", no_argument, nullptr, 'h'},
        {nullptr, 0, nullptr, 0},
    };

    while (true) {
        const int opt =
            getopt_long(argc, argv, "p:i:s:S:f:L:hR", long_options, nullptr);

        if (opt == -1) break;

        // clang-format off
        switch (opt) {
        case 'p': listenPort = stoi(optarg); break;
        case 'i': publicIp = optarg; break;
        case 's': storageUri = optarg; break;
        case 'R': sendReliably = true; break;
        case 'S': samplesPerPixel = stoi(optarg); break;
        case 'L': rayActionsLogRate = stof(optarg); break;
        case 'f': finishedRayAction = (FinishedRayAction)stoi(optarg); break;
        case 'h': usage(argv[0], EXIT_SUCCESS); break;
        default: usage(argv[0], EXIT_FAILURE);
        }
        // clang-format on
    }

    if (listenPort == 0 || rayActionsLogRate < 0 || rayActionsLogRate > 1.0 ||
        publicIp.empty() || storageUri.empty()) {
        usage(argv[0], EXIT_FAILURE);
    }

    unique_ptr<LambdaWorker> worker;

    try {
        worker = make_unique<LambdaWorker>(
            publicIp, listenPort, storageUri, sendReliably, samplesPerPixel,
            finishedRayAction, rayActionsLogRate);
        worker->run();
    } catch (const exception& e) {
        LOG(INFO) << argv[0] << ": " << e.what();
        exit_status = EXIT_FAILURE;
    }

    if (worker) {
        worker->uploadLogs();
    }

    return exit_status;
}
