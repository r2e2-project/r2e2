/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#include "loop.h"

#include <glog/logging.h>
#include <chrono>
#include <stdexcept>

#include "cloud/stats.h"
#include "net/http_response_parser.h"
#include "net/util.h"
#include "util/chunk.h"
#include "util/exception.h"
#include "util/optional.h"

using namespace std;
using namespace std::chrono;
using namespace PollerShortNames;

ExecutionLoop::ExecutionLoop()
    : signals_({SIGCHLD, SIGCONT, SIGHUP, SIGTERM, SIGQUIT}),
      signal_fd_(signals_) {
    signals_.set_as_mask();

    poller_.add_action(Poller::Action(
        signal_fd_.fd(), Direction::In,
        [&]() { return handle_signal(signal_fd_.read_signal()); },
        [&]() {
            return (child_processes_.size() > 0 or connections_.size() > 0 or
                    ssl_connections_.size() > 0);
        }));
}

// Minimum, where negative numbers are regarded as infinitely positive.
int min_neg_infinity(int a, int b) {
    if (a < 0) {
        return b;
    }
    if (b < 0) {
        return a;
    }
    return min(a, b);
}

Poller::Result ExecutionLoop::loop_once(const int timeout_ms) {
    // timeouts treat -1 as positive infinity
    int min_timeout_ms = timeout_ms;
    for (const auto &udp_connection : udp_connections_) {
        // If this connection is not within pace, it requests a timeout when it
        // would be, so that we can re-poll and schedule it.
        const int64_t millis_ahead_of_pace =
            udp_connection->micros_ahead_of_pace() / 1000;
        const int this_conn_timeout_ms =
            millis_ahead_of_pace <= 0 ? -1 : millis_ahead_of_pace;

        min_timeout_ms = min_neg_infinity(min_timeout_ms, this_conn_timeout_ms);
    }
    return poller_.poll(min_timeout_ms);
}

template <>
typename list<shared_ptr<TCPConnection>>::iterator
ExecutionLoop::create_connection(TCPSocket &&socket) {
    return connections_.emplace(connections_.end(),
                                make_shared<TCPConnection>(move(socket)));
}

template <>
typename list<shared_ptr<SSLConnection>>::iterator
ExecutionLoop::create_connection(NBSecureSocket &&socket) {
    return ssl_connections_.emplace(ssl_connections_.end(),
                                    make_shared<SSLConnection>(move(socket)));
}

template <>
void ExecutionLoop::remove_connection<TCPConnection>(
    const list<shared_ptr<TCPConnection>>::iterator &it) {
    connections_.erase(it);
}

template <>
void ExecutionLoop::remove_connection<SSLConnection>(
    const list<shared_ptr<SSLConnection>>::iterator &it) {
    ssl_connections_.erase(it);
}

template <>
void ExecutionLoop::remove_connection<UDPConnection>(
    const list<shared_ptr<UDPConnection>>::iterator &it) {
    udp_connections_.erase(it);
}

template <>
shared_ptr<TCPConnection> ExecutionLoop::add_connection(
    TCPSocket &&socket,
    const function<bool(shared_ptr<TCPConnection>, string &&)> &data_callback,
    const function<void()> &error_callback,
    const function<void()> &close_callback) {
    auto connection_it = create_connection<TCPSocket>(move(socket));
    shared_ptr<TCPConnection> &connection = *connection_it;

    auto real_close_callback = [connection_it, cc = move(close_callback),
                                this]() {
        cc();
        remove_connection<TCPConnection>(connection_it);
    };

    auto fderror_callback = [error_callback, real_close_callback] {
        error_callback();
        real_close_callback();
    };

    poller_.add_action(Poller::Action(
        connection->socket_, Direction::Out,
        [connection]() {
            string::const_iterator last_write =
                connection->socket_.write(connection->write_buffer_.begin(),
                                          connection->write_buffer_.cend());
            const auto bytes_sent =
                last_write - connection->write_buffer_.cbegin();
            connection->bytes_sent += bytes_sent;
            connection->write_buffer_.erase(0, bytes_sent);

            return ResultType::Continue;
        },
        [connection] { return connection->write_buffer_.size(); },
        fderror_callback));

    poller_.add_action(Poller::Action(
        connection->socket_, Direction::In,
        [connection, data_callback{move(data_callback)},
         close_callback{move(real_close_callback)}]() {
            string data{move(connection->socket_.read())};
            connection->bytes_received += data.length();

            if (data.empty() or not data_callback(connection, move(data))) {
                close_callback();
                return ResultType::CancelAll;
            }

            return ResultType::Continue;
        },
        [connection]() { return true; }, fderror_callback));

    return *connection_it;
}

template <>
shared_ptr<SSLConnection> ExecutionLoop::add_connection(
    NBSecureSocket &&socket,
    const function<bool(shared_ptr<SSLConnection>, string &&)> &data_callback,
    const function<void()> &error_callback,
    const function<void()> &close_callback) {
    const auto connection_it = create_connection<NBSecureSocket>(move(socket));
    shared_ptr<SSLConnection> &connection = *connection_it;

    auto real_close_callback = [connection_it, cc = move(close_callback),
                                this]() {
        cc();
        remove_connection<SSLConnection>(connection_it);
    };

    auto fderror_callback = [error_callback, real_close_callback] {
        error_callback();
        real_close_callback();
    };

    poller_.add_action(Poller::Action(
        connection->socket_, Direction::Out,
        [connection]() {
            connection->socket_.ezwrite(move(connection->write_buffer_));
            connection->write_buffer_ = string{};
            return ResultType::Continue;
        },
        [connection] { return connection->write_buffer_.size(); },
        fderror_callback));

    poller_.add_action(Poller::Action(
        connection->socket_, Direction::In,
        [connection, data_callback = move(data_callback),
         close_callback = move(real_close_callback)]() {
            string data{move(connection->socket_.ezread())};

            if (data.empty() or not data_callback(connection, move(data))) {
                close_callback();
                return ResultType::CancelAll;
            }

            return ResultType::Continue;
        },
        [connection]() { return true; }, fderror_callback));

    return *connection_it;
}

template <>
shared_ptr<TCPConnection> ExecutionLoop::make_connection(
    const Address &address,
    const function<bool(shared_ptr<TCPConnection>, string &&)> &data_callback,
    const function<void()> &error_callback,
    const function<void()> &close_callback) {
    TCPSocket socket;
    socket.set_blocking(false);
    socket.connect_nonblock(address);

    return add_connection<TCPSocket>(move(socket), data_callback,
                                     error_callback, close_callback);
}

template <>
shared_ptr<SSLConnection> ExecutionLoop::make_connection(
    const Address &address,
    const function<bool(shared_ptr<SSLConnection>, string &&)> &data_callback,
    const function<void()> &error_callback,
    const function<void()> &close_callback) {
    TCPSocket socket;
    socket.set_blocking(false);
    socket.connect_nonblock(address);
    NBSecureSocket secure_socket{
        move(ssl_context_.new_secure_socket(move(socket)))};
    secure_socket.connect();

    return add_connection<NBSecureSocket>(move(secure_socket), data_callback,
                                          error_callback, close_callback);
}

shared_ptr<UDPConnection> ExecutionLoop::make_udp_connection(
    const function<bool(shared_ptr<UDPConnection>, Address &&, string &&)>
        &data_callback,
    const function<void()> &error_callback,
    const function<void()> &close_callback, const bool pacing) {
    UDPSocket socket;
    socket.set_blocking(false);

    auto connection_it = udp_connections_.emplace(
        udp_connections_.end(),
        make_shared<UDPConnection>(move(socket), pacing));

    shared_ptr<UDPConnection> &connection = *connection_it;

    auto real_close_callback = [connection_it, cc = move(close_callback),
                                this]() {
        cc();
        remove_connection<UDPConnection>(connection_it);
    };

    auto fderror_callback = [error_callback, real_close_callback] {
        error_callback();
        real_close_callback();
    };

    poller_.add_action(Poller::Action(
        connection->socket_, Direction::In,
        [connection, data_callback{move(data_callback)},
         close_callback{move(real_close_callback)}]() {
            auto datagram = connection->socket_.recvfrom();
            auto &data = datagram.second;

            connection->bytes_received += data.length();

            if (data.length() > 0) {
                Chunk chunk(data);
                auto first_byte = static_cast<PacketType>(chunk.octet());

                chunk = chunk(1);

                switch (first_byte) {
                case PacketType::Unreliable:
                    data = data.substr(1);
                    break;

                case PacketType::Reliable: {
                    if (chunk.size() >= 8) {
                        connection->to_be_acked_[datagram.first].push_back(
                            chunk.be64());
                        data = data.substr(9);
                    }

                    break;
                }

                case PacketType::Ack:
                    while (chunk.size()) {
                        connection->received_acks_.insert(chunk.be64());
                        chunk = chunk(8);
                    }

                    return ResultType::Continue;

                default:
                    throw runtime_error("invalid packet");
                }
            }

            if (not data_callback(connection, move(datagram.first),
                                  move(data))) {
                close_callback();
                return ResultType::CancelAll;
            }

            return ResultType::Continue;
        },
        [connection]() { return true; }, fderror_callback));

    poller_.add_action(Poller::Action(
        connection->socket_, Direction::Out,
        [connection]() {
            auto &datagram = connection->queue_front();
            connection->bytes_sent += datagram.data.length();
            connection->socket_.sendto(datagram.destination, datagram.data);

            if (!datagram.data.empty() and
                datagram.data[0] == to_underlying(PacketType::Reliable)) {
                connection->outstanding_packets_.emplace_back(
                    make_pair(steady_clock::now() + 10s, move(datagram)));
            }

            connection->queue_pop();
            return ResultType::Continue;
        },
        [connection] {
            return (not connection->queue_empty()) and
                   connection->within_pace();
        },
        fderror_callback));

    poller_.add_action(Poller::Action(
        connection->ackHandleTimer.fd, Direction::In,
        [connection]() {
            RECORD_INTERVAL("handleAcks");
            connection->ackHandleTimer.reset();

            // sending acknowledgements
            for (auto &ackkv : connection->to_be_acked_) {
                string ack;

                for (size_t i = 0; i < ackkv.second.size(); i++) {
                    ack += put_field(ackkv.second[i]);

                    if (ack.length() >= 1'400 or i == ackkv.second.size() - 1) {
                        connection->enqueue_datagram(ackkv.first, move(ack),
                                                     PacketPriority::High,
                                                     PacketType::Ack);

                        ack = {};
                    }
                }
            }

            connection->to_be_acked_.clear();

            // processing received acks
            if (connection->received_acks_.empty() or
                connection->outstanding_packets_.empty()) {
                return ResultType::Continue;
            }

            const auto now = steady_clock::now();
            while (connection->outstanding_packets_.size() and
                   connection->outstanding_packets_.front().first <= now) {
                auto &packet = connection->outstanding_packets_.front().second;

                if (!connection->received_acks_.count(packet.sequence_number)) {
                    connection->enqueue_datagram(move(packet));
                } else {
                    connection->received_acks_.erase(packet.sequence_number);
                }

                connection->outstanding_packets_.pop_front();
            }

            return ResultType::Continue;
        },
        [connection] {
            return connection->to_be_acked_.size() or
                   (connection->received_acks_.size() and
                    connection->outstanding_packets_.size() and
                    connection->outstanding_packets_.front().first <=
                        steady_clock::now());
        },
        fderror_callback));

    return *connection_it;
}

template <class ConnectionType>
uint64_t ExecutionLoop::make_http_request(
    const string &tag, const Address &address, const HTTPRequest &request,
    HTTPResponseCallbackFunc response_callback,
    FailureCallbackFunc failure_callback) {
    const uint64_t connection_id = current_id_++;

    auto parser = make_shared<HTTPResponseParser>();
    parser->new_request_arrived(request);

    auto data_callback = [parser, connection_id, tag, response_callback](
                             shared_ptr<ConnectionType>, string &&data) {
        parser->parse(data);

        if (not parser->empty()) {
            response_callback(connection_id, tag, parser->front());
            parser->pop();
            return false;
        }

        return true;
    };

    auto error_callback = [connection_id, tag, failure_callback] {
        failure_callback(connection_id, tag);
    };

    auto close_callback = [] {};

    auto connection = make_connection<ConnectionType>(
        address, data_callback, error_callback, close_callback);

    connection->write_buffer_ = move(request.str());

    return connection_id;
}

uint64_t ExecutionLoop::make_listener(
    const Address &address,
    const function<bool(ExecutionLoop &, TCPSocket &&)> &connection_callback) {
    TCPSocket socket;
    socket.set_blocking(false);
    socket.set_reuseaddr();
    socket.bind(address);
    socket.listen();

    auto connection_it = create_connection<TCPSocket>(move(socket));
    shared_ptr<TCPConnection> &connection_ptr = *connection_it;

    poller_.add_action(Poller::Action(
        (*connection_it)->socket_, Direction::In,
        [connection_ptr, connection_it, connection_callback,
         this]() -> ResultType {
            if (not connection_callback(
                    *this, move(connection_ptr->socket_.accept()))) {
                remove_connection<TCPConnection>(connection_it);
                return ResultType::CancelAll;
            }

            return ResultType::Continue;
        }));

    return current_id_++;
}

uint64_t ExecutionLoop::add_child_process(const string &tag,
                                          LocalCallbackFunc callback,
                                          function<int()> &&child_procedure,
                                          const bool throw_if_failed) {
    child_processes_.emplace_back(current_id_, throw_if_failed, callback,
                                  ChildProcess(tag, move(child_procedure)));
    return current_id_++;
}

Poller::Action::Result ExecutionLoop::handle_signal(
    const signalfd_siginfo &sig) {
    switch (sig.ssi_signo) {
    case SIGCONT:
        for (auto &child : child_processes_) {
            get<3>(child).resume();
        }
        break;

    case SIGCHLD:
        if (child_processes_.empty()) {
            throw runtime_error(
                "received SIGCHLD without any managed children");
        }

        for (auto it = child_processes_.begin(); it != child_processes_.end();
             it++) {
            ChildProcess &child = get<3>(*it);

            if (child.terminated() or (not child.waitable())) {
                continue;
            }

            child.wait(true);

            if (child.terminated()) {
                if (get<1>(*it) and child.exit_status() != 0) {
                    child.throw_exception();
                }

                auto &callback = get<2>(*it);
                callback(get<0>(*it), child.name(), child.exit_status());

                it = child_processes_.erase(it);
                it--;
            } else if (not child.running()) {
                /* suspend parent too */
                CheckSystemCall("raise", raise(SIGSTOP));
            }
        }

        break;

    case SIGHUP:
    case SIGTERM:
    case SIGQUIT:
        throw runtime_error("interrupted by signal");

    default:
        throw runtime_error("unknown signal");
    }

    return ResultType::Continue;
}

template uint64_t ExecutionLoop::make_http_request<TCPConnection>(
    const string &, const Address &, const HTTPRequest &,
    HTTPResponseCallbackFunc, FailureCallbackFunc);

template uint64_t ExecutionLoop::make_http_request<SSLConnection>(
    const string &, const Address &, const HTTPRequest &,
    HTTPResponseCallbackFunc, FailureCallbackFunc);
