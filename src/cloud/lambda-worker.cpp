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

constexpr size_t UDP_MTU_BYTES{1'350};
constexpr char LOG_STREAM_ENVAR[] = "AWS_LAMBDA_LOG_STREAM_NAME";

#define TLOG(tag) LOG(INFO) << "[" #tag "] "

LambdaWorker::LambdaWorker(const string& coordinatorIP,
                           const uint16_t coordinatorPort,
                           const string& storageUri,
                           const WorkerConfiguration& config)
    : config(config),
      coordinatorAddr(coordinatorIP, coordinatorPort),
      workingDirectory("/tmp/pbrt-worker"),
      storageBackend(StorageBackend::create_backend(storageUri)) {
    cerr << "* starting worker in " << workingDirectory.name() << endl;
    roost::chdir(workingDirectory.name());

    FLAGS_log_dir = ".";
    FLAGS_log_prefix = false;
    google::InitGoogleLogging(logBase.c_str());

    TLOG(DIAG) << "start "
               << duration_cast<microseconds>(
                      workerDiagnostics.startTime.time_since_epoch())
                      .count();

    if (trackRays) {
        TLOG(RAY) << "pathID,hop,shadowRay,workerID,otherPartyID,treeletID,"
                     "outQueue,udpQueue,outstanding,timestamp,size,action";
    }

    if (trackPackets) {
        TLOG(PACKET) << "sourceID,destinationID,seqNo,attempt,size,"
                        "rayCount,timestamp,action";
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

    eventAction[Event::UdpReceive] = loop.poller().add_action(Poller::Action(
        udpConnection, Direction::In,
        bind(&LambdaWorker::handleUdpReceive, this), [this]() { return true; },
        []() { throw runtime_error("udp in failed"); }));

    eventAction[Event::RayAcks] = loop.poller().add_action(Poller::Action(
        handleRayAcknowledgementsTimer.fd, Direction::In,
        bind(&LambdaWorker::handleRayAcknowledgements, this),
        [this]() {
            return !toBeAcked.empty() ||
                   (!outstandingRayPackets.empty() &&
                    outstandingRayPackets.front().first <= packet_clock::now());
        },
        []() { throw runtime_error("acks failed"); }));

    eventAction[Event::UdpSend] = loop.poller().add_action(Poller::Action(
        udpConnection, Direction::Out, bind(&LambdaWorker::handleUdpSend, this),
        [this]() {
            return udpConnection.within_pace() &&
                   (!servicePackets.empty() || !rayPackets.empty() ||
                    outQueueSize > 0);
        },
        []() { throw runtime_error("udp out failed"); }));

    /* trace rays */
    eventAction[Event::RayQueue] = loop.poller().add_action(Poller::Action(
        dummyFD, Direction::Out, bind(&LambdaWorker::handleRayQueue, this),
        [this]() { return !rayQueue.empty(); },
        []() { throw runtime_error("ray queue failed"); }));

    /* send finished rays */
    /* FIXME we're throwing out finished rays, for now */
    eventAction[Event::FinishedQueue] = loop.poller().add_action(Poller::Action(
        dummyFD, Direction::Out, bind(&LambdaWorker::handleFinishedQueue, this),
        [this]() {
            // clang-format off
            switch (this->config.finishedRayAction) {
            case FinishedRayAction::Discard: return finishedQueue.size() > 5000;
            case FinishedRayAction::SendBack: return !finishedQueue.empty();
            default: return false;
            }
            // clang-format on
        },
        []() { throw runtime_error("finished queue failed"); }));

    /* handle peers */
    eventAction[Event::Peers] = loop.poller().add_action(Poller::Action(
        peerTimer.fd, Direction::In, bind(&LambdaWorker::handlePeers, this),
        [this]() { return !peers.empty(); },
        []() { throw runtime_error("peers failed"); }));

    /* handle received messages */
    eventAction[Event::Messages] = loop.poller().add_action(Poller::Action(
        dummyFD, Direction::Out, bind(&LambdaWorker::handleMessages, this),
        [this]() { return !messageParser.empty(); },
        []() { throw runtime_error("messages failed"); }));

    /* send updated stats */
    eventAction[Event::WorkerStats] = loop.poller().add_action(Poller::Action(
        workerStatsTimer.fd, Direction::In,
        bind(&LambdaWorker::handleWorkerStats, this), [this]() { return true; },
        []() { throw runtime_error("worker stats failed"); }));

    /* record diagnostics */
    eventAction[Event::Diagnostics] = loop.poller().add_action(Poller::Action(
        workerDiagnosticsTimer.fd, Direction::In,
        bind(&LambdaWorker::handleDiagnostics, this), [this]() { return true; },
        []() { throw runtime_error("handle diagnostics failed"); }));

    loop.poller().add_action(
        Poller::Action(reconnectTimer.fd, Direction::In,
                       bind(&LambdaWorker::handleReconnects, this),
                       [this]() { return !reconnectRequests.empty(); },
                       []() { throw runtime_error("reconnect failed"); }));

    /* request new peers for neighboring treelets */
    /* loop.poller().add_action(Poller::Action(
        dummyFD, Direction::Out,
        bind(&LambdaWorker::handleNeededTreelets, this),
        [this]() { return !neededTreelets.empty(); },
        []() { throw runtime_error("treelet request failed"); })); */

    /* send back finished paths */
    /* loop.poller().add_action(
        Poller::Action(finishedPathsTimer.fd, Direction::In,
                       bind(&LambdaWorker::handleFinishedPaths, this),
                       [this]() { return !finishedPathIds.empty(); },
                       []() { throw runtime_error("finished paths failed");
       }));*/

    coordinatorConnection->enqueue_write(
        Message::str(0, OpCode::Hey, safe_getenv_or(LOG_STREAM_ENVAR, "")));
}

void LambdaWorker::NetStats::merge(const NetStats& other) {
    bytesSent += other.bytesSent;
    bytesReceived += other.bytesReceived;
    packetsSent += other.packetsSent;
    packetsReceived += other.packetsReceived;
}

void LambdaWorker::initBenchmark(const uint32_t duration,
                                 const uint32_t destination,
                                 const uint32_t rate) {
    /* (1) disable all unnecessary actions */
    set<uint64_t> toDeactivate{
        eventAction[Event::RayQueue],   eventAction[Event::FinishedQueue],
        eventAction[Event::Peers],      eventAction[Event::NeededTreelets],
        eventAction[Event::UdpSend],    eventAction[Event::UdpReceive],
        eventAction[Event::RayAcks],    eventAction[Event::Diagnostics],
        eventAction[Event::WorkerStats]};

    loop.poller().deactivate_actions(toDeactivate);
    udpConnection.reset_reference();

    if (rate) {
        udpConnection.set_rate(rate);
    }

    /* (2) set up new udpReceive and udpSend actions */
    eventAction[Event::UdpReceive] = loop.poller().add_action(Poller::Action(
        udpConnection, Direction::In,
        [this]() {
            auto datagram = udpConnection.recvfrom();
            benchmarkData.checkpoint.bytesReceived += datagram.second.length();
            benchmarkData.checkpoint.packetsReceived++;
            return ResultType::Continue;
        },
        [this]() { return true; },
        []() { throw runtime_error("udp in failed"); }));

    if (destination) {
        const Address address = peers.at(destination).address;

        eventAction[Event::UdpSend] = loop.poller().add_action(Poller::Action(
            udpConnection, Direction::Out,
            [this, address]() {
                const static string packet =
                    Message::str(*workerId, OpCode::Ping, string(1300, 'x'));
                udpConnection.sendto(address, packet);

                benchmarkData.checkpoint.bytesSent += packet.length();
                benchmarkData.checkpoint.packetsSent++;

                return ResultType::Continue;
            },
            [this]() { return udpConnection.within_pace(); },
            []() { throw runtime_error("udp out failed"); }));
    }

    benchmarkTimer = make_unique<TimerFD>(seconds{duration});
    checkpointTimer = make_unique<TimerFD>(seconds{1});

    loop.poller().add_action(Poller::Action(
        benchmarkTimer->fd, Direction::In,
        [this, destination]() {
            benchmarkTimer->reset();
            benchmarkData.end = probe_clock::now();

            set<uint64_t> toDeactivate{eventAction[Event::UdpReceive],
                                       eventAction[Event::NetStats]};

            if (destination) toDeactivate.insert(eventAction[Event::UdpSend]);
            loop.poller().deactivate_actions(toDeactivate);

            return ResultType::CancelAll;
        },
        [this]() { return true; },
        []() { throw runtime_error("benchmark timer failed"); }));

    eventAction[Event::NetStats] = loop.poller().add_action(Poller::Action(
        checkpointTimer->fd, Direction::In,
        [this]() {
            checkpointTimer->reset();
            benchmarkData.checkpoint.timestamp = probe_clock::now();
            benchmarkData.checkpoints.push_back(benchmarkData.checkpoint);
            benchmarkData.stats.merge(benchmarkData.checkpoint);
            benchmarkData.checkpoint = {};

            return ResultType::Continue;
        },
        [this]() { return true; },
        []() { throw runtime_error("net stats failed"); }));

    benchmarkData.start = probe_clock::now();
    benchmarkData.checkpoint.timestamp = benchmarkData.start;
}

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

void LambdaWorker::logPacket(const uint64_t sequenceNumber,
                             const uint16_t attempt, const PacketAction action,
                             const WorkerId otherParty, const size_t packetSize,
                             const size_t numRays) {
    if (!trackPackets) return;

    ostringstream oss;

    switch (action) {
    case PacketAction::Queued:
    case PacketAction::Sent:
    case PacketAction::Acked:
    case PacketAction::AckSent:
        oss << *workerId << ',' << otherParty << ',';
        break;

    case PacketAction::Received:
    case PacketAction::AckReceived:
        oss << otherParty << ',' << *workerId << ',';
        break;

    default:
        throw runtime_error("invalid packet action");
    }

    oss << sequenceNumber << ',' << attempt << ',' << packetSize << ','
        << numRays << ','
        << duration_cast<microseconds>(rays_clock::now().time_since_epoch())
               .count()
        << ',';

    // clang-format off
    switch (action) {
    case PacketAction::Queued:      oss << "Queued";      break;
    case PacketAction::Sent:        oss << "Sent";        break;
    case PacketAction::Received:    oss << "Received";    break;
    case PacketAction::Acked:       oss << "Acked";       break;
    case PacketAction::AckSent:     oss << "AckSent";     break;
    case PacketAction::AckReceived: oss << "AckReceived"; break;

    default: throw runtime_error("invalid packet action");
    }
    // clang-format on

    TLOG(PACKET) << oss.str();
}

void LambdaWorker::logRayAction(const RayState& state, const RayAction action,
                                const WorkerId otherParty) {
    if (!trackRays || !state.trackRay) return;

    ostringstream oss;

    // clang-format off

    /* pathID,hop,shadowRay,workerID,otherPartyID,treeletID,outQueue,udpQueue,
       outstanding,timestamp,size,action */

    oss << state.sample.id << ','
        << state.hop << ','
        << state.isShadowRay << ','
        << *workerId << ','
        << ((action == RayAction::Sent ||
             action == RayAction::Received) ? otherParty
                                            : *workerId) << ','
        << state.CurrentTreelet() << ','
        << outQueueSize << ','
        << (servicePackets.size() + rayPackets.size()) << ','
        << outstandingRayPackets.size() << ','
        << duration_cast<microseconds>(
               rays_clock::now().time_since_epoch()).count() << ','
        << state.Size() << ',';
    // clang-format on

    // clang-format off
    switch (action) {
    case RayAction::Generated: oss << "Generated"; break;
    case RayAction::Traced:    oss << "Traced";    break;
    case RayAction::Queued:    oss << "Queued";    break;
    case RayAction::Pending:   oss << "Pending";   break;
    case RayAction::Sent:      oss << "Sent";      break;
    case RayAction::Received:  oss << "Received";  break;
    case RayAction::Finished:  oss << "Finished";  break;

    default: throw runtime_error("invalid ray action");
    }
    // clang-format on

    TLOG(RAY) << oss.str();
}

ResultType LambdaWorker::handleRayQueue() {
    RECORD_INTERVAL("handleRayQueue");

    auto recordFinishedPath = [this](const uint64_t pathId) {
        this->workerStats.recordFinishedPath();
        this->finishedPathIds.push_back(pathId);
    };

    deque<RayStatePtr> processedRays;

    constexpr size_t MAX_RAYS = 3'000;

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
                    workerStats.recordFinishedRay(*newRayPtr);
                    finishedQueue.push_back(move(newRayPtr));
                } else {
                    processedRays.push_back(move(newRayPtr));
                }
            } else if (!emptyVisit || hit) {
                processedRays.push_back(move(newRayPtr));
            } else if (emptyVisit) {
                newRay.Ld = 0.f;
                logRayAction(*newRayPtr, RayAction::Finished);
                workerStats.recordFinishedRay(*newRayPtr);
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
                workerStats.recordFinishedRay(*rayPtr);
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
            ray->Serialize();

            if (treeletToWorker.count(nextTreelet)) {
                logRayAction(*ray, RayAction::Queued);
                workerStats.recordSendingRay(*ray);
                outQueueLengthBytes[nextTreelet] += ray->SerializedSize();
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

ResultType LambdaWorker::handleFinishedPaths() {
    RECORD_INTERVAL("handleFinishedPaths");
    finishedPathsTimer.reset();

    string payload;
    for (const auto pathId : finishedPathIds) {
        payload += put_field(pathId);
    }

    finishedPathIds.clear();

    coordinatorConnection->enqueue_write(
        Message::str(*workerId, OpCode::FinishedPaths, payload));

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

    switch (config.finishedRayAction) {
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
            Message::str(*workerId, OpCode::FinishedRays, oss.str()));

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
            servicePackets.emplace_front(peer.address, peer.id, message.str());
            peer.tries++;
            break;
        }

        case Worker::State::Connected:
            /* send keep alive */
            /* if (peerId > 0 && peer.nextKeepAlive < now) {
                peer.nextKeepAlive += KEEP_ALIVE_INTERVAL;
                servicePackets.emplace_back(
                    peer.address, peer.id,
                    Message::str(*workerId, OpCode::Ping,
                                 put_field(*workerId)));
            }*/

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
        Message message(*workerId, OpCode::GetWorker,
                        protoutil::to_string(proto));
        coordinatorConnection->enqueue_write(message.str());
        requestedTreelets.insert(treeletId);
    }

    neededTreelets.clear();
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

ResultType LambdaWorker::handleRayAcknowledgements() {
    handleRayAcknowledgementsTimer.reset();

    /* count the number of active senders to this worker */
    const auto activeSendersCount = max<uint32_t>(
        1, count_if(activeSenders.begin(), activeSenders.end(),
                    [now = packet_clock::now()](const auto& x) {
                        return (now - x.second) <= INACTIVITY_THRESHOLD;
                    }));

    trafficShare = max(1'000'000ul, config.maxUdpRate / activeSendersCount);

    for (const auto& addr : toBeAcked) {
        auto& receivedSeqNos = receivedPacketSeqNos[addr];

        /* Let's construct the ack message for this worker */
        string ack{};
        ack += put_field(trafficShare);
        ack += put_field(receivedSeqNos.smallest_not_in_set());

        for (auto sIt = receivedSeqNos.set().cbegin();
             sIt != receivedSeqNos.set().cend() && ack.length() < UDP_MTU_BYTES;
             sIt++) {
            ack += put_field(*sIt);
        }

        const bool tracked = packetLogBD(randEngine);
        string message = Message::str(*workerId, OpCode::Ack, move(ack), false,
                                      ackId, tracked);
        servicePackets.emplace_back(addr, addressToWorker[addr], move(message),
                                    true, ackId, tracked);

        ackId++;
    }

    toBeAcked.clear();

    // re-queue timed-out packets
    const auto now = packet_clock::now();

    while (!outstandingRayPackets.empty() &&
           outstandingRayPackets.front().first <= now) {
        auto& packet = outstandingRayPackets.front().second;
        auto& thisReceivedAcks = receivedAcks[packet.destination];

        workerStats.netStats.rtt +=
            duration_cast<milliseconds>(now - packet.sentAt);

        if (!thisReceivedAcks.contains(packet.sequenceNumber)) {
            packet.incrementAttempts();
            packet.retransmission = true;

            if (packet.attempt % 6 == 0) {
                reconnectRequests.insert(packet.destinationId);
            }

            rayPackets.push_back(move(packet));
        }

        outstandingRayPackets.pop_front();
    }

    return ResultType::Continue;
}

ResultType LambdaWorker::handleUdpSend() {
    RECORD_INTERVAL("sendUDP");

    if (!servicePackets.empty()) {
        auto& datagram = servicePackets.front();
        udpConnection.sendto(datagram.destination, datagram.data);

        if (datagram.ackPacket && datagram.tracked) {
            logPacket(datagram.ackId, 0, PacketAction::AckSent,
                      datagram.destinationId, datagram.data.length());
        }

        servicePackets.pop_front();
        return ResultType::Continue;
    }

    auto sendRayPacket = [this](Worker& peer, RayPacket&& packet) {
        peer.pacer.record_send(packet.length);

        udpConnection.sendmsg(packet.destination, packet.iov(),
                              packet.iovCount(), packet.length);

        packet.sentAt = packet_clock::now();
        peer.diagnostics.bytesSent += packet.length;
        workerStats.netStats.packetsSent++;

        /* do the necessary logging */
        for (auto& rayPtr : packet.rays) {
            logRayAction(*rayPtr, RayAction::Sent, packet.destinationId);
        }

        if (trackPackets && packet.tracked) {
            logPacket(packet.sequenceNumber, packet.attempt, PacketAction::Sent,
                      packet.destinationId, packet.length, packet.rays.size());
        }

        if (packet.reliable) {
            outstandingRayPackets.emplace_back(
                packet_clock::now() + PACKET_TIMEOUT, move(packet));
        }
    };

    for (auto it = rayPackets.begin(); it != rayPackets.end(); it++) {
        RayPacket& packet = *it;
        auto& peer = peers.at(packet.destinationId);
        if (peer.pacer.within_pace()) {
            sendRayPacket(peer, move(packet));
            rayPackets.erase(it);
            return ResultType::Continue;
        }
    }

    vector<TreeletId> myTreelets;
    myTreelets.reserve(outQueue.size());

    for (const auto& kv : outQueue) {
        myTreelets.push_back(kv.first);
    }

    random_shuffle(myTreelets.begin(), myTreelets.end());

    for (const auto& treeletId : myTreelets) {
        /* (1) pick a treelet randomly */
        auto& queue = outQueue[treeletId];
        auto& queueLengthBytes = outQueueLengthBytes[treeletId];

        /* (2) pick a worker to send to */
        WorkerId peerId;

        if (workerForTreelet.count(treeletId) &&
            workerForTreelet[treeletId].second >= packet_clock::now()) {
            peerId = workerForTreelet[treeletId].first;
        } else {
            const auto& candidates = treeletToWorker[treeletId];
            peerId = *random::sample(candidates.begin(), candidates.end());
            workerForTreelet[treeletId].first = peerId;
            workerForTreelet[treeletId].second =
                packet_clock::now() + TREELET_PEER_TIMEOUT;
        }

        auto& peer = peers.at(peerId);

        if (!peer.pacer.within_pace()) {
            continue;
        }

        auto& peerSeqNo = sequenceNumbers[peer.address];

        /* (3) collect the rays to fill a packet */
        const bool tracked = packetLogBD(randEngine);

        RayPacket packet(peer.address, peer.id, treeletId, queueLengthBytes,
                         config.sendReliably, peerSeqNo, tracked);

        while (!queue.empty()) {
            auto& ray = queue.front();
            const auto size = ray->SerializedSize();

            if (size == 0) {
                throw runtime_error("ray is not serialized");
            }

            if (size + packet.length > UDP_MTU_BYTES) break;

            packet.addRay(move(ray));

            queue.pop_front();
            queueLengthBytes -= size;
            outQueueSize--;
        }

        if (queue.empty()) {
            outQueue.erase(treeletId);
        }

        /* (4) fix the header */
        Message::str(packet.header, *workerId, OpCode::SendRays,
                     packet.length - Message::HEADER_LENGTH,
                     config.sendReliably, peerSeqNo, tracked);

        if (tracked) {
            logPacket(peerSeqNo, 0, PacketAction::Queued, peer.id,
                      packet.length, packet.rays.size());
        }

        peerSeqNo++;

        sendRayPacket(peer, move(packet));
        break;
    }

    return ResultType::Continue;
}

ResultType LambdaWorker::handleUdpReceive() {
    RECORD_INTERVAL("receiveUDP");

    auto datagram = udpConnection.recvfrom();
    auto& data = datagram.second;

    messageParser.parse(data);
    auto& messages = messageParser.completed_messages();

    auto it = messages.end();
    while (it != messages.begin()) {
        it--;
        auto& message = *it;

        if (message.is_read()) break;
        message.set_read();

        if (message.reliable()) {
            const auto seqNo = message.sequence_number();
            toBeAcked.insert(datagram.first);

            auto& received = receivedPacketSeqNos[datagram.first];

            if (message.tracked()) {
                logPacket(message.sequence_number(), message.attempt(),
                          PacketAction::Received, message.sender_id(),
                          message.total_length());
            }

            if (received.contains(seqNo)) {
                it = messages.erase(it);
                continue;
            } else {
                received.insert(seqNo);
            }
        }

        if (message.opcode() == OpCode::Ack) {
            Chunk chunk(message.payload());
            auto& thisReceivedAcks = receivedAcks[datagram.first];

            auto& peer = peers.at(message.sender_id());
            peer.pacer.set_rate(chunk.be32());
            chunk = chunk(4);

            if (message.tracked()) {
                logPacket(message.sequence_number(), message.attempt(),
                          PacketAction::AckReceived, message.sender_id(),
                          message.total_length(), 0u);
            }

            if (chunk.size() >= 8) {
                const auto next = chunk.be64();
                chunk = chunk(8);

                thisReceivedAcks.insertAllBelow(next);

                while (chunk.size()) {
                    thisReceivedAcks.insert(chunk.be64());
                    chunk = chunk(8);
                }
            }

            it = messages.erase(it);
        } else if (message.opcode() == OpCode::SendRays) {
            auto& peer = peers.at(message.sender_id());
            peer.diagnostics.bytesReceived += message.total_length();
            activeSenders[message.sender_id()] = packet_clock::now();
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
    qStats.queuedUdp = rayPackets.size() + servicePackets.size();

    auto proto = to_protobuf(workerStats);

    proto.set_timestamp_us(
        duration_cast<microseconds>(now() - workerStats.startTime).count());

    Message message{*workerId, OpCode::WorkerStats,
                    protoutil::to_string(proto)};
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

    auto proto = to_protobuf(workerDiagnostics);

    for (auto& peerkv : peers) {
        auto& peer = peerkv.second;

        if (peer.diagnostics.bytesSent || peer.diagnostics.bytesReceived) {
            auto tdata = proto.add_traffic_data();
            tdata->set_worker_id(peer.id);
            tdata->set_bytes_sent(peer.diagnostics.bytesSent);
            tdata->set_bytes_received(peer.diagnostics.bytesReceived);

            peer.diagnostics = {};
        }
    }

    for (auto& outkv : outQueue) {
        auto treeletId = outkv.first;
        auto& queue = outkv.second;

        auto tdata = proto.add_treelet_data();
        tdata->set_treelet_id(treeletId);
        tdata->set_queued_rays(queue.size());
    }

    TLOG(DIAG) << timestamp << " " << protoutil::to_json(proto);

    workerDiagnostics.reset();

    return ResultType::Continue;
}

void LambdaWorker::generateRays(const Bounds2i& bounds) {
    const Bounds2i sampleBounds = camera->film->GetSampleBounds();
    const Vector2i sampleExtent = sampleBounds.Diagonal();
    const auto samplesPerPixel = sampler->samplesPerPixel;
    const Float rayScale = 1 / sqrt((Float)samplesPerPixel);

    /* for ray tracking */
    bernoulli_distribution bd{config.rayActionsLogRate};

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
                (pixel.x + pixel.y * sampleExtent.x) * config.samplesPerPixel +
                sample;
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
                statePtr->Serialize();

                if (treeletToWorker.count(nextTreelet)) {
                    logRayAction(state, RayAction::Queued);
                    workerStats.recordSendingRay(state);
                    outQueueLengthBytes[nextTreelet] += state.SerializedSize();
                    outQueue[nextTreelet].push_back(move(statePtr));
                    outQueueSize++;
                } else {
                    logRayAction(state, RayAction::Pending);
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
                    auto& treeletOutLen = outQueueLengthBytes[treeletId];

                    outQueueSize += treeletPending.size();
                    pendingQueueSize -= treeletPending.size();

                    while (!treeletPending.empty()) {
                        auto& front = treeletPending.front();
                        workerStats.recordSendingRay(*front);
                        treeletOutLen += front->SerializedSize();
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
        const uint32_t queueLen = *reinterpret_cast<uint32_t const*>(data);
        data += sizeof(uint32_t);

        const uint32_t dataLen = message.payload().length() - sizeof(uint32_t);
        uint32_t offset = 0;

        while (offset < dataLen) {
            const auto len = *reinterpret_cast<const uint32_t*>(data + offset);
            offset += 4;

            RayStatePtr ray = make_unique<RayState>();
            ray->Deserialize(data + offset, len);
            ray->hop++;
            offset += len;

            logRayAction(*ray, RayAction::Received, message.sender_id());
            pushRayQueue(move(ray));
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
        const int64_t ahead_us = udpConnection.micros_ahead_of_pace();

        int64_t ahead_ms = ahead_us / 1000;
        if (ahead_us != 0 && ahead_ms == 0) ahead_ms = 1;

        const int timeout_ms = udpConnection.within_pace() ? -1 : ahead_ms;

        min_timeout_ms = min_neg_infinity(min_timeout_ms, timeout_ms);

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
    sampler = sampler::from_protobuf(proto_sampler, config.samplesPerPixel);

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

    benchmarkData.stats.merge(benchmarkData.checkpoint);

    TLOG(BENCH) << "start "
                << duration_cast<milliseconds>(
                       benchmarkData.start.time_since_epoch())
                       .count();

    TLOG(BENCH) << "end "
                << duration_cast<milliseconds>(
                       benchmarkData.end.time_since_epoch())
                       .count();

    for (const auto& item : benchmarkData.checkpoints) {
        TLOG(BENCH) << "checkpoint "
                    << duration_cast<milliseconds>(
                           item.timestamp.time_since_epoch())
                           .count()
                    << " " << item.bytesSent << " " << item.bytesReceived << " "
                    << item.packetsSent << " " << item.packetsReceived;
    }

    TLOG(BENCH) << "stats " << benchmarkData.stats.bytesSent << " "
                << benchmarkData.stats.bytesReceived << " "
                << benchmarkData.stats.packetsSent << " "
                << benchmarkData.stats.packetsReceived;

    google::FlushLogFiles(google::INFO);

    vector<storage::PutRequest> putLogsRequest = {
        {infoLogName, logPrefix + to_string(*workerId) + ".INFO"}};

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
         << "  -M --max-udp-rate RATE     maximum UDP rate (Mbps)" << endl
         << "  -S --samples N             number of samples per pixel" << endl
         << "  -L --log-rays RATE         log ray actions" << endl
         << "  -P --log-packets RATE      log packets" << endl
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
    uint64_t maxUdpRate = 80_Mbps;
    int samplesPerPixel = 0;
    FinishedRayAction finishedRayAction = FinishedRayAction::Discard;
    float rayActionsLogRate = 0.0;
    float packetsLogRate = 0.0;

    struct option long_options[] = {
        {"port", required_argument, nullptr, 'p'},
        {"ip", required_argument, nullptr, 'i'},
        {"storage-backend", required_argument, nullptr, 's'},
        {"reliable-udp", no_argument, nullptr, 'R'},
        {"samples", required_argument, nullptr, 'S'},
        {"log-rays", required_argument, nullptr, 'L'},
        {"log-packets", required_argument, nullptr, 'P'},
        {"finished-ray", required_argument, nullptr, 'f'},
        {"max-udp-rate", required_argument, nullptr, 'M'},
        {"help", no_argument, nullptr, 'h'},
        {nullptr, 0, nullptr, 0},
    };

    while (true) {
        const int opt = getopt_long(argc, argv, "p:i:s:S:f:L:P:M:hR",
                                    long_options, nullptr);

        if (opt == -1) break;

        // clang-format off
        switch (opt) {
        case 'p': listenPort = stoi(optarg); break;
        case 'i': publicIp = optarg; break;
        case 's': storageUri = optarg; break;
        case 'R': sendReliably = true; break;
        case 'M': maxUdpRate = stoull(optarg) * 1'000'000; break;
        case 'S': samplesPerPixel = stoi(optarg); break;
        case 'L': rayActionsLogRate = stof(optarg); break;
        case 'P': packetsLogRate = stof(optarg); break;
        case 'f': finishedRayAction = (FinishedRayAction)stoi(optarg); break;
        case 'h': usage(argv[0], EXIT_SUCCESS); break;
        default: usage(argv[0], EXIT_FAILURE);
        }
        // clang-format on
    }

    if (listenPort == 0 || rayActionsLogRate < 0 || rayActionsLogRate > 1.0 ||
        packetsLogRate < 0 || packetsLogRate > 1.0 || publicIp.empty() ||
        storageUri.empty() || maxUdpRate == 0) {
        usage(argv[0], EXIT_FAILURE);
    }

    unique_ptr<LambdaWorker> worker;
    WorkerConfiguration config{sendReliably,      maxUdpRate,
                               samplesPerPixel,   finishedRayAction,
                               rayActionsLogRate, packetsLogRate};

    try {
        worker =
            make_unique<LambdaWorker>(publicIp, listenPort, storageUri, config);
        worker->run();
    } catch (const exception& e) {
        cerr << argv[0] << ": " << e.what() << endl;
        exit_status = EXIT_FAILURE;
    }

    if (worker) {
        worker->uploadLogs();
    }

    return exit_status;
}
