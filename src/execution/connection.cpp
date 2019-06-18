#include "connection.h"

#include "net/util.h"

using namespace std;
using namespace std::chrono;

int64_t UDPConnection::micros_ahead_of_pace() const {
    if (!pacing_) return -1;

    const auto now = steady_clock::now();
    const int64_t elapsed_micros =
        duration_cast<microseconds>(now - rate_reference_pt_).count();
    const int64_t elapsed_micros_if_at_pace =
        (bits_since_reference_ / (1e6 * rate_bps_));
    return elapsed_micros_if_at_pace - elapsed_micros;
}

void UDPConnection::reset_reference() {
    if (!pacing_) return;
    rate_reference_pt_ = steady_clock::now();
    bits_since_reference_ = 0;
}

void UDPConnection::record_send(const size_t data_len) {
    if (!pacing_) return;

    bits_since_reference_ += data_len * 8;
    if (std::chrono::steady_clock::now() >=
        rate_reference_pt_ + reference_reset_time_) {
        reset_reference();
    }
}
