/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#ifndef PBRT_EXECUTION_CONNECTION_H
#define PBRT_EXECUTION_CONNECTION_H

#include <chrono>
#include <deque>
#include <iostream>
#include <queue>
#include <string>
#include <tuple>
#include <unordered_map>

#include "net/address.h"
#include "net/nb_secure_socket.h"
#include "net/socket.h"
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

struct PacketData {
    Address destination;
    std::string data;
    PacketPriority priority{PacketPriority::Normal};
    bool reliable{false};

    PacketData(const Address& addr, std::string&& data,
               const PacketPriority p = PacketPriority::Normal,
               const bool reliable = false)
        : destination(addr),
          data(move(data)),
          priority(p),
          reliable(reliable) {}
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

    uint64_t rate_Mb_per_s_{10};
    uint64_t bits_since_reference_{0};
    std::chrono::steady_clock::time_point rate_reference_pt_{
        std::chrono::steady_clock::now()};
    std::chrono::microseconds reference_reset_time_{1'000'000};

    /* reliable transport */
    uint64_t sequence_number_{0};
    std::unordered_map<
        uint64_t, std::pair<PacketData, std::chrono::steady_clock::time_point>>
        outstanding_packets_{};

  public:
    enum class FirstByte : uint8_t {
        Unreliable,
        Reliable,
        Ack
    };

    UDPConnection() {}

    UDPConnection(UDPSocket&& sock, const bool pacing = false)
        : socket_(std::move(sock)), pacing_(pacing) {}

    UDPConnection& operator=(const UDPConnection&) = delete;
    UDPConnection(const UDPConnection&) = delete;

    void enqueue_datagram(const Address& addr, std::string&& data,
                          const PacketPriority p = PacketPriority::Normal,
                          const bool reliable = false);

    // How many microseconds are we ahead of our pace?
    int64_t micros_ahead_of_pace() const {
        if (!pacing_) return -1;
        auto now = std::chrono::steady_clock::now();
        int64_t elapsed_micros =
            std::chrono::duration_cast<std::chrono::microseconds>(
                now - rate_reference_pt_)
                .count();
        int64_t elapsed_micros_if_at_pace =
            bits_since_reference_ / rate_Mb_per_s_;
        return elapsed_micros_if_at_pace - elapsed_micros;
    }

    bool within_pace() { return micros_ahead_of_pace() <= 0; }

    bool queue_empty() { return outgoing_datagrams_.empty(); }

    const PacketData& queue_front() const { return outgoing_datagrams_.top(); }

    void queue_pop() {
        if (pacing_) {
            const PacketData& sent = outgoing_datagrams_.top();
            bits_since_reference_ += sent.data.length() * 8;
            if (std::chrono::steady_clock::now() >=
                rate_reference_pt_ + reference_reset_time_) {
                reset_reference();
            }
        }
        outgoing_datagrams_.pop();
    }

    size_t queue_size() { return outgoing_datagrams_.size(); }

    UDPSocket& socket() { return socket_; }

    size_t bytes_sent{0};
    size_t bytes_received{0};

  private:
    void reset_reference() {
        if (!pacing_) return;
        rate_reference_pt_ = std::chrono::steady_clock::now();
        bits_since_reference_ = 0;
    }
};

using TCPConnection = Connection<TCPSocket>;
using SSLConnection = Connection<NBSecureSocket>;

#endif /* PBRT_EXECUTION_CONNECTION_H */
