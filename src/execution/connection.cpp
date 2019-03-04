#include "connection.h"

#include "net/util.h"

using namespace std;

void UDPConnection::enqueue_datagram(const Address& addr, string&& data,
                                     const PacketPriority p,
                                     const PacketType first_byte) {
    string header(1, to_underlying(first_byte));
    uint64_t seqno = 0;

    if (first_byte == PacketType::Reliable) {
        seqno = sequence_number_++;
        header += put_field(seqno);
    }

    data.insert(0, header);
    outgoing_datagrams_.emplace(addr, move(data), p, seqno);
}

void UDPConnection::enqueue_datagram(PacketData&& packet) {
    outgoing_datagrams_.push(move(packet));
}

int64_t UDPConnection::micros_ahead_of_pace() const {
    if (!pacing_) return -1;

    auto now = std::chrono::steady_clock::now();
    int64_t elapsed_micros =
        std::chrono::duration_cast<std::chrono::microseconds>(
            now - rate_reference_pt_)
            .count();
    int64_t elapsed_micros_if_at_pace = bits_since_reference_ / rate_Mb_per_s_;
    return elapsed_micros_if_at_pace - elapsed_micros;
}

void UDPConnection::queue_pop() {
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

void UDPConnection::reset_reference() {
    if (!pacing_) return;
    rate_reference_pt_ = std::chrono::steady_clock::now();
    bits_since_reference_ = 0;
}
