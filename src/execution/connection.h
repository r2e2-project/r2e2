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

class UDPConnection {
    friend class ExecutionLoop;

  private:
    UDPSocket socket_{};
    std::queue<std::pair<Address, std::string>> packet_queue_;

    /* pacing */
    bool pacing_{false};
    uint64_t rate_Mb_per_s_{160};
    uint64_t bits_since_reference_{0};
    std::chrono::steady_clock::time_point rate_reference_pt_{
        std::chrono::steady_clock::now()};
    std::chrono::microseconds reference_reset_time_{1'000'000};

  public:
    UDPConnection(const bool pacing = false) : pacing_(pacing) {
        socket_.set_blocking(false);
    }

    UDPConnection& operator=(const UDPConnection&) = delete;
    UDPConnection(const UDPConnection&) = delete;

    int64_t micros_ahead_of_pace() const;
    bool within_pace() { return micros_ahead_of_pace() <= 0; }
    void record_send(const size_t data_len);

    UDPSocket& socket() { return socket_; }

    size_t bytes_sent{0};
    size_t bytes_received{0};

    void enqueue_packet(const Address& addr, std::string&& data) {
        packet_queue_.emplace(addr, move(data));
    }

  private:
    void reset_reference();
};

using TCPConnection = Connection<TCPSocket>;
using SSLConnection = Connection<NBSecureSocket>;

#endif /* PBRT_EXECUTION_CONNECTION_H */
