#include "connection.h"

#include <cmath>

#include "net/util.h"

using namespace std;
using namespace chrono;

int64_t Pacer::micros_ahead_of_pace() const {
    if (!enabled_) return -1;

    const auto T =
        nanoseconds{static_cast<int64_t>(ceil(1e9 * packet_size_ / rate_))};

    return duration_cast<microseconds>((last_sent_ + T) - clock::now()).count();
}

void Pacer::set_rate(const uint64_t rate) { rate_ = rate; }

void Pacer::record_send(const size_t data_len) {
    if (!enabled_) return;

    packet_size_ = 0.75 * packet_size_ + 0.25 * data_len * 8;
    last_sent_ = clock::now();
}

pair<Address, string> UDPConnection::recvfrom(void) {
    auto result = UDPSocket::recvfrom();
    bytes_received += result.second.length();
    return result;
}

void UDPConnection::send(const string& payload) {
    record_send(payload.length());
    bytes_sent += payload.length();
    UDPSocket::send(payload);
}

void UDPConnection::sendto(const Address& peer, const string& payload) {
    record_send(payload.length());
    bytes_sent += payload.length();
    UDPSocket::sendto(peer, payload);
}

void UDPConnection::sendmsg(const Address& peer, const iovec* iov,
                            const size_t iovcnt) {
    size_t total_length = 0;
    for (size_t i = 0; i < iovcnt; i++) {
        total_length += iov[i].iov_len;
    }

    sendmsg(peer, iov, iovcnt, total_length);
}

void UDPConnection::sendmsg(const Address& peer, const iovec* iov,
                            const size_t iovcnt, const size_t total_length) {
    record_send(total_length);
    bytes_sent += total_length;
    UDPSocket::sendmsg(peer, iov, iovcnt);
}
