#ifndef PBRT_UTIL_HISTOGRAM_H
#define PBRT_UTIL_HISTOGRAM_H

#include <string>
#include <vector>

template <class T>
class Histogram {
  private:
    T width;
    T minimum;
    T maximum;
    std::vector<size_t> bins;

  public:
    Histogram(const T width, const T minimum, const T maximum);
    void add(const T value);

    std::string str() const;
};

#endif /* PBRT_UTIL_HISTOGRAM_H */
