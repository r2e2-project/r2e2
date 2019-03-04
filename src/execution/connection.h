/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#ifndef PBRT_EXECUTION_CONNECTION_H
#define PBRT_EXECUTION_CONNECTION_H

#include <chrono>
#include <deque>
#include <iostream>
#include <map>
#include <queue>
#include <set>
#include <string>
#include <tuple>
#include <unordered_map>

#include "net/address.h"
#include "net/nb_secure_socket.h"
#include "net/socket.h"
#include "util/timerfd.h"
#include "util/util.h"

class ExecutionLoop;

template <class SocketType>
class Connection {
    friend class ExecutionLoop;

  private:
    SocketType socket_{};
    std::string write_buffer_{};

  public:
    Connection() {}

    Connection(SocketType&& sock) : socket_(std::move(sock)) {}

    Connection& operator=(const Connection&) = delete;
    Connection(const Connection&) = delete;

    ~Connection() {
        if (write_buffer_.size()) {
            /* std::cerr << "Connection destroyed with data left in write
             * buffer" << std::endl; */
        }
    }

    void enqueue_write(const std::string& str) { write_buffer_.append(str); }
    SocketType& socket() { return socket_; }

    size_t bytes_sent{0};
    size_t bytes_received{0};
};

enum class PacketPriority { Low, Normal, High };

enum class PacketType : uint8_t { Unreliable, Reliable, Ack };

struct PacketData {
    Address destination;
    std::string data;
    PacketPriority priority{PacketPriority::Normal};
    uint64_t sequence_number{0};

    PacketData(const Address& addr, std::string&& data,
               const PacketPriority p = PacketPriority::Normal,
               const uint64_t sequence_number = 0)
        : destination(addr),
          data(move(data)),
          priority(p),
          sequence_number(sequence_number) {}
};

struct PacketCompare {
    bool operator()(const PacketData& a, const PacketData& b) {
        return to_underlying(a.priority) < to_underlying(b.priority);
    }
};

class UDPConnection {
    friend class ExecutionLoop;

  private:
    UDPSocket socket_{};
    std::priority_queue<PacketData, std::deque<PacketData>, PacketCompare>
        outgoing_datagrams_{};

    bool pacing_{false};

    uint64_t rate_Mb_per_s_{80};
    uint64_t bits_since_reference_{0};
    std::chrono::steady_clock::time_point rate_reference_pt_{
        std::chrono::steady_clock::now()};
    std::chrono::microseconds reference_reset_time_{1'000'000};

    /* reliable transport */
    uint64_t sequence_number_{0};
    TimerFD ackHandleTimer{std::chrono::seconds{2}};
    std::deque<std::pair<std::chrono::steady_clock::time_point, PacketData>>
        outstanding_packets_{};

    std::map<Address, std::vector<uint64_t>> to_be_acked_{};
    std::set<uint64_t> received_acks_{};

  public:
    UDPConnection() {}

    UDPConnection(UDPSocket&& sock, const bool pacing = false)
        : socket_(std::move(sock)), pacing_(pacing) {}

    UDPConnection& operator=(const UDPConnection&) = delete;
    UDPConnection(const UDPConnection&) = delete;

    void enqueue_datagram(const Address& addr, std::string&& data,
                          const PacketPriority p = PacketPriority::Normal,
                          const PacketType first_byte = PacketType::Unreliable);

    void enqueue_datagram(PacketData&& packet);

    // How many microseconds are we ahead of our pace?
    int64_t micros_ahead_of_pace() const;
    bool within_pace() { return micros_ahead_of_pace() <= 0; }

    bool queue_empty() { return outgoing_datagrams_.empty(); }
    void queue_pop();
    size_t queue_size() { return outgoing_datagrams_.size(); }
    const PacketData& queue_front() const { return outgoing_datagrams_.top(); }

    UDPSocket& socket() { return socket_; }

    size_t bytes_sent{0};
    size_t bytes_received{0};

  private:
    void reset_reference();
};

using TCPConnection = Connection<TCPSocket>;
using SSLConnection = Connection<NBSecureSocket>;

#endif /* PBRT_EXECUTION_CONNECTION_H */
