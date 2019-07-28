#include "connection.h"

#include "net/util.h"

using namespace std;
using namespace chrono;

int64_t RateLimiter::micros_ahead_of_pace() const {
    if (!enabled_) return -1;

    const auto now = steady_clock::now();
    const int64_t elapsed_micros =
        duration_cast<microseconds>(now - rate_reference_pt_).count();
    const int64_t elapsed_micros_if_at_pace =
        bits_since_reference_ / rate_Mbps_;
    return elapsed_micros_if_at_pace - elapsed_micros;
}

void RateLimiter::reset_reference() {
    if (!enabled_) return;
    rate_reference_pt_ = steady_clock::now();
    bits_since_reference_ = 0;
}

void RateLimiter::record_send(const size_t data_len) {
    if (!enabled_) return;

    bits_since_reference_ += data_len * 8;
    if (steady_clock::now() >= rate_reference_pt_ + reference_reset_time_) {
        reset_reference();
    }
}

pair<Address, string> UDPConnection::recvfrom(void) {
    auto result = UDPSocket::recvfrom();
    bytes_received += result.second.length();
    return result;
}

void UDPConnection::send(const string& payload) {
    rate_limiter.record_send(payload.length());
    bytes_sent += payload.length();
    UDPSocket::send(payload);
}

void UDPConnection::sendto(const Address& peer, const string& payload) {
    rate_limiter.record_send(payload.length());
    bytes_sent += payload.length();
    UDPSocket::sendto(peer, payload);
}

void UDPConnection::sendmsg(const Address& peer, const iovec* iov,
                            const size_t iovcnt, const size_t total_length) {
    rate_limiter.record_send(total_length);
    bytes_sent += total_length;
    UDPSocket::sendmsg(peer, iov, iovcnt);
}
