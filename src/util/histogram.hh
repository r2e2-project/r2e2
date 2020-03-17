#pragma once

#include <limits>
#include <string>
#include <vector>

#include "r2t2.pb.h"

template <class T>
class Histogram {
  private:
    T width_;
    T lower_bound_;
    T upper_bound_;
    std::vector<size_t> bins_{};

    uint64_t count_ = 0;
    T min_value_ = std::numeric_limits<T>::max();
    T max_value_ = std::numeric_limits<T>::min();
    T sum_ = 0;
    T squares_sum_ = 0;

  public:
    Histogram(const r2t2::protobuf::HistogramUInt64& proto);
    Histogram(const T width, const T lower_bound, const T upper_bound);

    void add(const T value);

    T width() const { return width_; }
    T lower_bound() const { return lower_bound_; }
    T upper_bound() const { return upper_bound_; }
    const std::vector<size_t>& bins() const { return bins_; }
    uint64_t count() const { return count_; }
    T min_value() const { return min_value_; }
    T max_value() const { return max_value_; }
    T sum() const { return sum_; }
    T squares_sum() const { return squares_sum_; }

    std::string str() const;
    r2t2::protobuf::HistogramUInt64 to_protobuf() const;
};
