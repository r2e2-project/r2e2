#include "util/histogram.h"

#include <sstream>
#include <stdexcept>

using namespace std;

template <class T>
Histogram<T>::Histogram(const T width, const T minimum, const T maximum)
    : width(width), minimum(minimum), maximum(maximum) {
    bins.resize(static_cast<size_t>((maximum - minimum + width) / width));
}

template <class T>
void Histogram<T>::add(const T value) {
    if (value < minimum || value > maximum) {
        throw runtime_error("value < minimum || value > maximum");
    }

    count++;
    maxValue = max(value, maxValue);
    minValue = min(value, minValue);
    sum += value;
    squaresSum += value * value;

    const size_t bin = static_cast<size_t>((value - minimum) / width);
    bins[bin]++;
}

template <class T>
string Histogram<T>::str() const {
    if (count == 0) {
        return "{}";
    }

    ostringstream oss;
    size_t lastPos;

    for (lastPos = bins.size() - 1; lastPos < bins.size(); lastPos--) {
        if (bins[lastPos] != 0) break;
    }

    const double average = 1.0 * sum / count;
    const double stddev = 1.0 * squaresSum / count - average * average;

    oss << R"({"width":)" << width << R"(,"min":)" << minValue << R"(,"max":)"
        << maxValue << R"(,"count":)" << count << R"(,"avg":)" << average
        << R"(,"std":)" << stddev << R"(,"bins":[)";

    for (size_t i = 0; i < bins.size() && i <= lastPos; i++) {
        if (i > 0) oss << ',';
        oss << bins[i];
    }

    oss << "]}";

    return oss.str();
}

template class Histogram<uint8_t>;
template class Histogram<uint16_t>;
template class Histogram<uint32_t>;
template class Histogram<uint64_t>;
