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
