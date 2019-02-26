#include "connection.h"

using namespace std;

string put_field(const uint64_t n) {
    const uint64_t network_order = htobe64(n);
    return string(reinterpret_cast<const char*>(&network_order),
                  sizeof(network_order));
}

void UDPConnection::enqueue_datagram(const Address& addr, string&& data,
                                     const PacketPriority p,
                                     const bool reliable) {
    string header(1, to_underlying(FirstByte::Unreliable));

    if (reliable) {
        header[0] = to_underlying(FirstByte::Reliable);
        header += put_field(sequence_number_++);
    }

    data.insert(0, header);
    outgoing_datagrams_.emplace(addr, move(data), p, reliable);
}
