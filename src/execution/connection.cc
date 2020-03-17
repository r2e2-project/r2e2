#include "connection.hh"

#include <cmath>

#include "net/util.hh"

using namespace std;
using namespace chrono;

int64_t Pacer::micros_ahead_of_pace() const {
    if (!enabled_) return -1;

    const auto now = pacer_clock::now();
    const auto promise_end =
        ref_time_ + microseconds{static_cast<uint64_t>(
                        ceil(1e6 * bits_since_ref_ / rate_))};

    return duration_cast<microseconds>(promise_end - now).count();
}

void Pacer::set_rate(const uint64_t rate) { rate_ = rate; }

void Pacer::record_send(const size_t data_len) {
    if (!enabled_) return;

    if (within_pace()) {
        ref_time_ = pacer_clock::now();
        bits_since_ref_ = data_len * 8;
    } else {
        bits_since_ref_ += data_len * 8;
    }
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
