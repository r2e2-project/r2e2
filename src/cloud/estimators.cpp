#include "cloud/estimators.h"

#include <algorithm>
#include <numeric>

using namespace std;

pair<double, double> meanAndStandardDev(const vector<double>& xs) {
    // There are various stream algorithms, but I think that they're less
    // numerically stable than this.
    double mean = std::accumulate(xs.begin(), xs.end(), 0.0) / xs.size();
    double sumDeviationsSquared = std::accumulate(
        xs.begin(), xs.end(), 0.0,
        [mean](double acc, double x) { return acc + (mean - x) * (mean - x); });
    return std::make_pair(mean, sqrt(sumDeviationsSquared / xs.size()));
}
