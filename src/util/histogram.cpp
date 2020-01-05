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

    const size_t bin = static_cast<size_t>((value - minimum) / width);
    bins[bin]++;
}

template <class T>
string Histogram<T>::str() const {
    ostringstream oss;

    oss << "{'width':" << width << ",'min':" << minimum << ",'max':" << maximum;
    oss << ",'bins':[";

    for (size_t i = 0; i < bins.size(); i++) {
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
