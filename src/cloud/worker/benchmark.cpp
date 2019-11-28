#include "cloud/lambda-worker.h"

using namespace std;
using namespace meow;
using namespace std::chrono;
using namespace pbrt;
using namespace PollerShortNames;

using OpCode = Message::OpCode;
using PollerResult = Poller::Result::Type;

void LambdaWorker::initBenchmark(const uint32_t duration,
                                 const uint32_t destination,
                                 const uint32_t rate) {
    /* (1) disable all unnecessary actions */
    set<uint64_t> toDeactivate{
        eventAction[Event::TraceQueue],     eventAction[Event::OutQueue],
        eventAction[Event::FinishedQueue],  eventAction[Event::Peers],
        eventAction[Event::NeededTreelets], eventAction[Event::UdpSend],
        eventAction[Event::UdpReceive],     eventAction[Event::RayAcks],
        eventAction[Event::Diagnostics],    eventAction[Event::WorkerStats],
        eventAction[Event::LogLeases]};

    loop.poller().deactivate_actions(toDeactivate);

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
