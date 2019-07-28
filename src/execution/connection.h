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

    ~Connection() {}

    void enqueue_write(const std::string& str) { write_buffer_.append(str); }
    SocketType& socket() { return socket_; }

    size_t bytes_sent{0};
    size_t bytes_received{0};
};

class Pacer {
  private:
    uint64_t rate_Mbps_{80};
    uint64_t bits_since_reference_{0};
    std::chrono::steady_clock::time_point rate_reference_pt_{
        std::chrono::steady_clock::now()};
    std::chrono::microseconds reference_reset_time_{1'000'000};

    bool enabled_;

  public:
    Pacer(const bool enabled, const uint64_t rate_mbps)
        : enabled_(enabled), rate_Mbps_(rate_mbps) {}

    int64_t micros_ahead_of_pace() const;
    bool within_pace() { return micros_ahead_of_pace() <= 0; }
    void set_rate(const uint64_t rate) { rate_Mbps_ = rate; }
    void reset_reference();
    void record_send(const size_t data_len);
};

class UDPConnection : public Pacer, public UDPSocket {
    friend class ExecutionLoop;

  private:
    std::queue<std::pair<Address, std::string>> packet_queue_;

  public:
    UDPConnection(const bool pacing = false, const uint64_t rate_mbps = 80)
        : Pacer(pacing, rate_mbps), UDPSocket() {
        set_blocking(false);
    }

    UDPConnection& operator=(const UDPConnection&) = delete;
    UDPConnection(const UDPConnection&) = delete;

    /* UDPSocket methods */
    std::pair<Address, std::string> recvfrom(void);
    void send(const std::string& payload);
    void sendto(const Address& peer, const std::string& payload);
    void sendmsg(const Address& peer, const iovec* iov, const size_t iovcnt,
                 const size_t total_length);

    size_t bytes_sent{0};
    size_t bytes_received{0};
};

using TCPConnection = Connection<TCPSocket>;
using SSLConnection = Connection<NBSecureSocket>;

#endif /* PBRT_EXECUTION_CONNECTION_H */
