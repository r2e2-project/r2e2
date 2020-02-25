/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#ifndef PBRT_NET_WS_SERVER_H
#define PBRT_NET_WS_SERVER_H

#include <deque>
#include <functional>
#include <map>
#include <set>

#include "address.h"
#include "http_request_parser.h"
#include "nb_secure_socket.h"
#include "socket.h"
#include "util/poller.h"
#include "ws_message_parser.h"

/* this implementation is not thread-safe. */
template <class SocketType>
class WSServer {
  public:
    using MessageCallback =
        std::function<void(const uint64_t, const WSMessage &)>;
    using OpenCallback = std::function<void(const uint64_t)>;
    using CloseCallback = std::function<void(const uint64_t)>;

  private:
    uint64_t last_connection_id_{0};

    struct Connection {
        enum class State {
            NotConnected = 0,
            Connecting,
            Connected,
            Closing,
            Closed
        } state{State::NotConnected};

        SocketType socket;

        /* incoming messages */
        HTTPRequestParser ws_handshake_parser{};
        WSMessageParser ws_message_parser{};

        /* outgoing messages */
        std::deque<std::string> send_buffer{};
        size_t send_buffer_offset{0};

        Connection(TCPSocket &&sock);

        std::string read();
        void write();

        /* the connection has data to write to TCPSocket directly,
         * or write to NBSecureSocket's internal send_buffer */
        bool data_to_write() const { return send_buffer.size() > 0; }

        /* tell the poller if the connection is interested in sending
         * i.e., it or its NBSecureSocket has pending data in the send_buffer */
        bool interested_in_sending() const;

        unsigned int buffer_bytes() const;
        void clear_buffer();
    };

    TCPSocket listener_socket_{};
    Address listener_addr_{};
    std::map<uint64_t, Connection> connections_{};
    Poller &poller_;

    MessageCallback message_callback_{};
    OpenCallback open_callback_{};
    CloseCallback close_callback_{};

    std::set<uint64_t> closed_connections_{};

    void init_listener_socket();

    /* gracefully close the connection */
    void wait_close_connection(const uint64_t connection_id);

    /* force close the connection */
    void force_close_connection(const uint64_t connection_id);

  public:
    WSServer(const Address &listener_addr, Poller &poller);

    void set_message_callback(MessageCallback func) {
        message_callback_ = func;
    }
    void set_open_callback(OpenCallback func) { open_callback_ = func; }
    void set_close_callback(CloseCallback func) { close_callback_ = func; }

    bool queue_frame(const uint64_t connection_id, const WSFrame &frame);

    Address peer_addr(const uint64_t connection_id) const;

    unsigned int buffer_bytes(const uint64_t connection_id) const;
    void clear_buffer(const uint64_t connection_id);

    /* public method to gracefully close a connection */
    void close_connection(const uint64_t connection_id);

    /* force close an idle connection and no longer poll on its socket */
    void clean_idle_connection(const uint64_t connection_id);
};

using WebSocketTCPServer = WSServer<TCPSocket>;

#endif /* PBRT_NET_WS_SERVER_H */
