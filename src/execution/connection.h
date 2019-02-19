/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#ifndef PBRT_EXECUTION_CONNECTION_H
#define PBRT_EXECUTION_CONNECTION_H

#include <chrono>
#include <deque>
#include <iostream>
#include <queue>
#include <string>
#include <tuple>

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

using UDPPacketData = std::tuple<Address, std::string, PacketPriority>;

struct UDPPacketCompare {
    bool operator()(const UDPPacketData& a, const UDPPacketData& b) {
        return to_underlying(std::get<2>(a)) < to_underlying(std::get<2>(b));
    }
};

class UDPConnection {
    friend class ExecutionLoop;

  private:
    UDPSocket socket_{};
    std::priority_queue<UDPPacketData, std::deque<UDPPacketData>,
                        UDPPacketCompare>
        outgoing_datagrams_{};

    bool pacing_{false};

    uint64_t rate_Mb_per_s_{10};
    uint64_t bits_since_reference_{0};
    std::chrono::steady_clock::time_point rate_reference_pt_{std::chrono::steady_clock::now()};
    std::chrono::microseconds reference_reset_time_{1'000'000};


  public:
    UDPConnection() {}

    UDPConnection(UDPSocket&& sock, const bool pacing = false)
        : socket_(std::move(sock)), pacing_(pacing) {}

    UDPConnection& operator=(const UDPConnection&) = delete;
    UDPConnection(const UDPConnection&) = delete;

    void enqueue_datagram(const Address& addr, std::string&& datagram,
                          const PacketPriority p = PacketPriority::Normal) {
        outgoing_datagrams_.push(make_tuple(addr, move(datagram), p));
    }

    bool queue_empty() {
        return outgoing_datagrams_.empty() or (not within_pace());
    }

    const UDPPacketData& queue_front() const {
        return outgoing_datagrams_.top();
    }

    void queue_pop() {
        if (pacing_) {
            const UDPPacketData& sent = outgoing_datagrams_.top();
            bits_since_reference_ += std::get<1>(sent).length() * 8;
            if (std::chrono::steady_clock::now() >= rate_reference_pt_ + reference_reset_time_) {
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
    bool within_pace() {
        if (!pacing_) return true;
        auto now = std::chrono::steady_clock::now();
        uint64_t elapsed_micros =
            std::chrono::duration_cast<std::chrono::microseconds>(
                now - rate_reference_pt_)
                .count();
        return bits_since_reference_ <=
               rate_Mb_per_s_ * elapsed_micros;
    }
    void reset_reference() {
        if (!pacing_) return;
        rate_reference_pt_ = std::chrono::steady_clock::now();
        bits_since_reference_ = 0;
    }
};

using TCPConnection = Connection<TCPSocket>;
using SSLConnection = Connection<NBSecureSocket>;

#endif /* PBRT_EXECUTION_CONNECTION_H */
