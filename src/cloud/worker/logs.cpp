#include "cloud/lambda-worker.h"

#include "messages/utils.h"

using namespace std;
using namespace meow;
using namespace std::chrono;
using namespace pbrt;
using namespace pbrt::global;
using namespace PollerShortNames;

using OpCode = Message::OpCode;
using PollerResult = Poller::Result::Type;

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

    /* pathID,hop,shadowRay,workerID,otherPartyID,treeletID,outQueue,
       sendQueueBytes,udpQueue,outstanding,timestamp,size,action */

    oss << state.sample.id << ','
        << state.hop << ','
        << state.isShadowRay << ','
        << *workerId << ','
        << ((action == RayAction::Sent ||
             action == RayAction::Received) ? otherParty
                                            : *workerId) << ','
        << state.CurrentTreelet() << ','
        << outQueueSize << ','
        << outQueueBytes[state.CurrentTreelet()] << ','
        << (servicePackets.size() + retransmissionQueue.size() +
            sendQueueSize) << ','
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

ResultType LambdaWorker::handleLogLease() {
    leaseLogTimer.reset();

    const auto now = flushLeaseInfo(true, true);

    TLOG(GLEASE)
        << leaseInfo.granted.size() << " "
        << duration_cast<milliseconds>(leaseInfo.start - workStart).count()
        << " " << duration_cast<milliseconds>(now - workStart).count();

    for (const auto& kv : leaseInfo.granted) {
        TLOG(GLEASE) << kv.first << ' ' << kv.second;
    }

    TLOG(TLEASE)
        << leaseInfo.taken.size() << " "
        << duration_cast<milliseconds>(leaseInfo.start - workStart).count()
        << " " << duration_cast<milliseconds>(now - workStart).count();

    for (const auto& kv : leaseInfo.taken) {
        TLOG(TLEASE) << kv.first << ' ' << kv.second;
    }

    TLOG(BSENT)
        << leaseInfo.sent.size() << " "
        << duration_cast<milliseconds>(leaseInfo.start - workStart).count()
        << " " << duration_cast<milliseconds>(now - workStart).count();

    for (const auto& kv : leaseInfo.sent) {
        TLOG(BSENT) << kv.first << ' ' << (kv.second * 8);
    }

    TLOG(BRECV)
        << leaseInfo.received.size() << " "
        << duration_cast<milliseconds>(leaseInfo.start - workStart).count()
        << " " << duration_cast<milliseconds>(now - workStart).count();

    for (const auto& kv : leaseInfo.received) {
        TLOG(BRECV) << kv.first << ' ' << (kv.second * 8);
    }

    leaseInfo.granted = {};
    leaseInfo.taken = {};
    leaseInfo.sent = {};
    leaseInfo.received = {};
    leaseInfo.start = packet_clock::now();

    return ResultType::Continue;
}

ResultType LambdaWorker::handleWorkerStats() {
    RECORD_INTERVAL("handleWorkerStats");
    workerStatsTimer.reset();

    auto& qStats = workerStats.queueStats;
    qStats.ray = traceQueue.size();
    qStats.finished = finishedQueue.size();
    qStats.pending = pendingQueueSize;
    qStats.out = outQueueSize;
    qStats.connecting =
        count_if(peers.begin(), peers.end(), [](const auto& peer) {
            return peer.second.state == Worker::State::Connecting;
        });
    qStats.connected = peers.size() - qStats.connecting;
    qStats.outstandingUdp = outstandingRayPackets.size();
    qStats.queuedUdp = retransmissionQueue.size() + servicePackets.size();

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

    workerDiagnostics.outstandingUdp = retransmissionQueue.size();
    lastDiagnostics.bytesSent = udpConnection.bytes_sent;
    lastDiagnostics.bytesReceived = udpConnection.bytes_received;

    const auto timestamp =
        duration_cast<microseconds>(now() - workerDiagnostics.startTime)
            .count();

    auto proto = to_protobuf(workerDiagnostics);

    TLOG(DIAG) << timestamp << " " << protoutil::to_json(proto);

    workerDiagnostics.reset();

    return ResultType::Continue;
}

void LambdaWorker::uploadLogs() {
    if (!workerId.initialized()) return;

    if (benchmarkTimer != nullptr) {
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
                        << " " << item.bytesSent << " " << item.bytesReceived
                        << " " << item.packetsSent << " "
                        << item.packetsReceived;
        }

        TLOG(BENCH) << "stats " << benchmarkData.stats.bytesSent << " "
                    << benchmarkData.stats.bytesReceived << " "
                    << benchmarkData.stats.packetsSent << " "
                    << benchmarkData.stats.packetsReceived;
    }

    google::FlushLogFiles(google::INFO);

    vector<storage::PutRequest> putLogsRequest = {
        {infoLogName, logPrefix + to_string(*workerId) + ".INFO"}};

    storageBackend->put(putLogsRequest);
}
