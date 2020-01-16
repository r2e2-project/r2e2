#ifndef PBRT_UTIL_HISTOGRAM_H
#define PBRT_UTIL_HISTOGRAM_H

#include <limits>
#include <string>
#include <vector>

template <class T>
class Histogram {
  private:
    T width;
    T lowerBound;
    T upperBound;
    std::vector<size_t> bins;

    uint64_t count = 0;
    T maxValue = std::numeric_limits<T>::min();
    T minValue = std::numeric_limits<T>::max();
    T sum = 0;
    T squaresSum = 0;

  public:
    Histogram(const T width, const T lowerBound, const T upperBound);
    void add(const T value);

    std::string str() const;
};

#endif /* PBRT_UTIL_HISTOGRAM_H */
