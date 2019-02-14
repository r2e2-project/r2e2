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

    static constexpr std::chrono::microseconds pace_{5'000};
    bool pacing_{false};
    int packet_per_pace_{30};
    int sent_packets_{0};
    std::chrono::steady_clock::time_point when_next_;

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

    int ms_until_next() {
        if (!pacing_ or outgoing_datagrams_.size() == 0) {
            return -1;
        }

        auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(
                          when_next_ - std::chrono::steady_clock::now())
                          .count();

        return (millis < 0) ? 0 : millis;
    }

    bool queue_empty() {
        return outgoing_datagrams_.empty() or
               (pacing_ and ms_until_next() != 0);
    }

    const UDPPacketData& queue_front() const {
        return outgoing_datagrams_.top();
    }

    void queue_pop() {
        outgoing_datagrams_.pop();
        if (!pacing_) return;
        sent_packets_++;
        if (sent_packets_ >= packet_per_pace_) {
            when_next_ = std::chrono::steady_clock::now() + pace_;
            sent_packets_ = 0;
        }
    }

    size_t queue_size() { return outgoing_datagrams_.size(); }

    UDPSocket& socket() { return socket_; }

    size_t bytes_sent{0};
    size_t bytes_received{0};
};

using TCPConnection = Connection<TCPSocket>;
using SSLConnection = Connection<NBSecureSocket>;

#endif /* PBRT_EXECUTION_CONNECTION_H */
