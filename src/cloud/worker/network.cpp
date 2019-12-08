#include "cloud/lambda-worker.h"

#include "net/util.h"
#include "util/random.h"

using namespace std;
using namespace meow;
using namespace std::chrono;
using namespace pbrt;
using namespace pbrt::global;
using namespace PollerShortNames;

using OpCode = Message::OpCode;
using PollerResult = Poller::Result::Type;

size_t LambdaWorker::Lease::allocatedBits(
    const packet_clock::time_point now) const {
    return (allocation *
            duration_cast<milliseconds>(min(now, expiresAt) - start).count()) /
           1000;
}

LambdaWorker::packet_clock::time_point LambdaWorker::flushLeaseInfo(
    const bool granted, const bool taken) {
    const auto now = packet_clock::now();
    if (!config.logLeases) return now;

    auto flush = [&now](map<WorkerId, Lease>& leases,
                        map<WorkerId, size_t>& bytes) {
        for (auto& lease : leases) {
            bytes[lease.first] += lease.second.allocatedBits(now);
            lease.second.start = min(now, lease.second.expiresAt);
        }
    };

    if (granted) {
        flush(grantedLeases, leaseInfo.granted);
    }

    if (taken) {
        flush(takenLeases, leaseInfo.taken);
    }

    return now;
}

void LambdaWorker::grantLease(const WorkerId workerId,
                              const uint32_t queueSize) {
    auto& lease = grantedLeases[workerId];
    lease.queueSize = queueSize;
    lease.expiresAt = packet_clock::now() + INACTIVITY_THRESHOLD;
}

void LambdaWorker::takeLease(const WorkerId workerId, const uint32_t rate) {
    auto& peer = peers.at(workerId);
    peer.pacer.set_rate(rate);

    auto& lease = takenLeases[workerId];
    const auto now = packet_clock::now();
    lease.expiresAt = now + INACTIVITY_THRESHOLD;

    if (config.logLeases) {
        leaseInfo.taken[workerId] += lease.allocatedBits(now);
        lease.start = now;
    }

    lease.allocation = rate;
}

void LambdaWorker::rebalanceLeases() {
    const uint32_t trafficShare =
        max<uint32_t>(DEFAULT_SEND_RATE,
                      config.maxUdpRate / max<size_t>(1, grantedLeases.size()));

    uint32_t excess = 0;
    uint32_t bigCount = grantedLeases.size();

    for (auto& lease : grantedLeases) {
        if (8000 * lease.second.queueSize / trafficShare < 100) {
            lease.second.allocation = min<uint32_t>(
                trafficShare,
                max<uint32_t>(DEFAULT_SEND_RATE, 80 * lease.second.queueSize));
            lease.second.small = true;
            excess += trafficShare - lease.second.allocation;
            bigCount--;
        } else {
            lease.second.small = false;
        }
    }

    const uint32_t excessShare = (bigCount == 0) ? 0 : (excess / bigCount);

    for (auto& lease : grantedLeases) {
        if (!lease.second.small) {
            lease.second.allocation = trafficShare + excessShare;
        }
    }
}

void LambdaWorker::expireLeases() {
    auto now = packet_clock::now();

    flushLeaseInfo(true, false);

    for (auto it = grantedLeases.begin(); it != grantedLeases.end();) {
        const WorkerId workerId = it->first;
        auto& lease = it->second;

        if (lease.expiresAt <= now) {
            it = grantedLeases.erase(it);
        } else {
            it++;
        }
    }
}

ResultType LambdaWorker::handleRayAcknowledgements() {
    handleRayAcknowledgementsTimer.reset();

    rebalanceLeases();

    /* count the number of active senders to this worker */
    for (const auto& addr : toBeAcked) {
        auto& receivedSeqNos = receivedPacketSeqNos[addr];

        /* Let's construct the ack message for this worker */
        string ack{};
        ack += put_field(grantedLeases[addressToWorker[addr]].allocation);
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
    expireLeases();

    const auto now = packet_clock::now();

    // re-queue timed-out packets
    while (!outstandingRayPackets.empty() &&
           outstandingRayPackets.front().first <= now) {
        auto& packet = outstandingRayPackets.front().second;
        auto& thisReceivedAcks = receivedAcks[packet.destination()->second];

        workerStats.netStats.rtt +=
            duration_cast<milliseconds>(now - packet.sentAt());

        if (!thisReceivedAcks.contains(packet.sequenceNumber())) {
            packet.incrementAttempts();
            packet.setRetransmission(true);

            if (packet.attempt() % 6 == 0) {
                reconnectRequests.insert(packet.destination()->first);
            }

            retransmissionQueue.push_back(move(packet));
        } else {
            workerStats.recordAcknowledgedBytes(packet.targetTreelet(),
                                                packet.raysLength());
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
        if (config.logLeases) {
            leaseInfo.sent[peer.id] += packet.length();
        }

        peer.pacer.record_send(packet.length());

        udpConnection.sendmsg(packet.destination()->second,
                              packet.iov(*workerId), packet.iovCount(),
                              packet.length());

        packet.recordSendTime();
        peer.diagnostics.bytesSent += packet.length();
        workerStats.netStats.packetsSent++;

        workerStats.recordSentBytes(packet.targetTreelet(),
                                    packet.raysLength());

        if (!packet.retransmission()) {
            outQueueBytes[packet.targetTreelet()] -= packet.raysLength();
        }

        /* do the necessary logging */
        for (auto& rayPtr : packet.rays()) {
            logRayAction(*rayPtr, RayAction::Sent, packet.destination()->first);
        }

        if (trackPackets && packet.tracked()) {
            logPacket(packet.sequenceNumber(), packet.attempt(),
                      PacketAction::Sent, packet.destination()->first,
                      packet.length(), packet.rayCount());
        }

        if (packet.reliable()) {
            outstandingRayPackets.emplace_back(
                packet_clock::now() + PACKET_TIMEOUT, move(packet));
        }
    };

    for (auto it = retransmissionQueue.begin(); it != retransmissionQueue.end();
         it++) {
        RayPacket& packet = *it;
        auto& peer = peers.at(packet.destination()->first);
        if (peer.pacer.within_pace()) {
            sendRayPacket(peer, move(packet));
            retransmissionQueue.erase(it);
            return ResultType::Continue;
        }
    }

    vector<TreeletId> shuffledTreelets;
    shuffledTreelets.reserve(sendQueue.size());

    for (const auto& kv : sendQueue) {
        shuffledTreelets.push_back(kv.first);
    }

    random_shuffle(shuffledTreelets.begin(), shuffledTreelets.end());

    auto now = packet_clock::now();

    for (const auto& treeletId : shuffledTreelets) {
        /* (1) pick a treelet randomly */
        auto& queue = sendQueue[treeletId];

        /* (2) pick a worker to send to */
        WorkerId peerId;

        if (workerForTreelet.count(treeletId) &&
            workerForTreelet[treeletId].second >= now) {
            peerId = workerForTreelet[treeletId].first;
        } else {
            const auto& candidates = treeletToWorker[treeletId];
            peerId = *random::sample(candidates.begin(), candidates.end());
            workerForTreelet[treeletId].first = peerId;
            workerForTreelet[treeletId].second = now + WORKER_TREELET_TIME;
        }

        auto& peer = peers.at(peerId);

        /* (1) do we have a lease for this worker? */
        auto leaseIt = takenLeases.find(peerId);

        /* (2) is it expired? */
        if (leaseIt == takenLeases.end() ||
            (leaseIt->second.expiresAt <= now &&
             (takenLeases.erase(leaseIt), true))) {
            peer.pacer.set_rate(DEFAULT_SEND_RATE);
        }

        /* (3) are we within pace? */
        if (!peer.pacer.within_pace()) {
            continue;
        }

        auto& peerSeqNo = sequenceNumbers[peer.address];
        auto& packet = queue.front();

        packet.setDestination(peer.id, peer.address);
        packet.setSequenceNumber(peerSeqNo);
        packet.setQueueLength(outQueueBytes[treeletId]);

        sendRayPacket(peer, move(packet));

        peerSeqNo++;
        queue.pop_front();

        if (queue.empty()) {
            sendQueue.erase(treeletId);
        }

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

            const auto rate = chunk.be32();

            auto& peer = peers.at(message.sender_id());
            takeLease(message.sender_id(), rate);

            chunk = chunk(4);

            if (message.tracked()) {
                logPacket(message.sequence_number(), message.attempt(),
                          PacketAction::AckReceived, message.sender_id(),
                          message.total_length(), 0u);
            }

            if (chunk.size() >= 8) {
                thisReceivedAcks.insertAllBelow(chunk.be64());
                chunk = chunk(8);

                while (chunk.size()) {
                    thisReceivedAcks.insert(chunk.be64());
                    chunk = chunk(8);
                }
            }

            it = messages.erase(it);
        } else if (message.opcode() == OpCode::SendRays) {
            Chunk chunk(message.payload());
            const auto queueSize = chunk.le32();
            grantLease(message.sender_id(), queueSize);

            leaseInfo.received[message.sender_id()] += message.total_length();
        }
    }

    return ResultType::Continue;
}
