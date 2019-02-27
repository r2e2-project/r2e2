#include "net/util.h"

#include <endian.h>

using namespace std;

string put_field(const uint64_t n) {
    const uint64_t network_order = htobe64(n);
    return string(reinterpret_cast<const char *>(&network_order),
                  sizeof(network_order));
}

string put_field(const uint32_t n) {
    const uint32_t network_order = htobe32(n);
    return string(reinterpret_cast<const char *>(&network_order),
                  sizeof(network_order));
}
