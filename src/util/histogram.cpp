#include "util/histogram.h"

#include <sstream>
#include <stdexcept>

#include "messages/utils.h"

using namespace std;
using namespace pbrt;

template <class T>
Histogram<T>::Histogram(const T width, const T lower_bound, const T upper_bound)
    : width_(width), lower_bound_(lower_bound), upper_bound_(upper_bound) {
    bins_.resize(
        static_cast<size_t>((upper_bound_ - lower_bound_ + width_) / width_));
}

template <class T>
void Histogram<T>::add(const T value) {
    if (value < lower_bound_ || value > upper_bound_) {
        throw runtime_error("value < lower_bound || value > upper_bound");
    }

    count_++;
    min_value_ = min(value, min_value_);
    max_value_ = max(value, max_value_);
    sum_ += value;
    squares_sum_ += value * value;

    const size_t bin = static_cast<size_t>((value - lower_bound_) / width_);
    bins_[bin]++;
}

template <>
Histogram<uint64_t>::Histogram(const protobuf::HistogramUInt64& proto) {
    width_ = proto.width();
    lower_bound_ = proto.lower_bound();
    upper_bound_ = proto.upper_bound();
    count_ = proto.count();
    min_value_ = proto.min();
    max_value_ = proto.max();
    sum_ = proto.sum();
    squares_sum_ = proto.squares_sum();

    bins_.resize(
        static_cast<size_t>((upper_bound_ - lower_bound_ + width_) / width_));

    size_t i = 0;
    for (const auto bin : proto.bins()) {
        bins_[i++] = bin;
    }
}

template <>
protobuf::HistogramUInt64 Histogram<uint64_t>::to_protobuf() const {
    protobuf::HistogramUInt64 proto;

    proto.set_width(width());
    proto.set_lower_bound(lower_bound());
    proto.set_upper_bound(upper_bound());
    proto.set_count(count());
    proto.set_min(min_value());
    proto.set_max(max_value());
    proto.set_sum(sum());
    proto.set_squares_sum(squares_sum());

    size_t lastPos;
    for (lastPos = bins_.size() - 1; lastPos < bins_.size(); lastPos--) {
        if (bins_[lastPos] != 0) break;
    }

    *proto.mutable_bins() = {bins_.begin(), bins_.begin() + lastPos};

    return proto;
}

template <>
string Histogram<uint64_t>::str() const {
    return protoutil::to_json(to_protobuf());
}

template class Histogram<uint8_t>;
template class Histogram<uint16_t>;
template class Histogram<uint32_t>;
template class Histogram<uint64_t>;
