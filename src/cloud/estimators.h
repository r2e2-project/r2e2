#ifndef PBRT_CLOUD_ESTIMATORS_H
#define PBRT_CLOUD_ESTIMATORS_H

#include <math.h>
#include <chrono>
#include <vector>
#include <utility>

/**
 * This class maintains a moving sum of stream of values.
 *
 * This sum is exponentially smoothed.
 *
 * T must support addition and multiplication by doubles
 */
template <typename T>
class ExponentialMovingSum {
  public:
    ExponentialMovingSum(std::chrono::high_resolution_clock::duration period);

    // Add this value, now.
    void add(const T& value);

    // Get the current sum estimate
    const T& getSum() const { return sum; }

  private:
    // The period over which the moving sum is computed.
    // (e.g. the sum of the last `period`, exponentially smoothed)
    const std::chrono::high_resolution_clock::duration period;

    // The time that the last value was fed into the stream
    std::chrono::high_resolution_clock::time_point lastTime;

    // The current estimate of the moving sum
    T sum;
};

template <typename T>
ExponentialMovingSum<T>::ExponentialMovingSum(
    std::chrono::high_resolution_clock::duration period)
    : period{period},
      lastTime{std::chrono::high_resolution_clock::now()},
      sum{} {
    // No other members
}

template <typename T>
void ExponentialMovingSum<T>::add(const T& value) {
    const auto now = std::chrono::high_resolution_clock::now();
    const auto deltaT = now - lastTime;
    const double x = double(deltaT.count()) / double(period.count());
    sum = exp(-x) * sum + value;
    lastTime = now;
}

template <typename T>
class RateEstimator {
  public:
    // Construct a rate estimator. Initially the rate is 0.
    RateEstimator();
    // Submit a new delta to this rate estimator! Modifies the estimate.
    // The value of the `delta` should be how much stuff happened since the
    // last update.
    void update(const T& delta);
    // Gets the current estimate of the rate (/s).
    const T& getRate() const;

  private:
    std::chrono::high_resolution_clock::duration period;
    double secondsPerPeriod;
    ExponentialMovingSum<T> sum;
};

template <typename T>
RateEstimator<T>::RateEstimator()
    : period{std::chrono::duration_cast<
          std::chrono::high_resolution_clock::duration>(
          std::chrono::seconds(3))},
      secondsPerPeriod{
          1.0 / double(std::chrono::duration_cast<std::chrono::seconds>(period)
                           .count())},
      sum{period} {
    // No other members
}

template <typename T>
void RateEstimator<T>::update(const T& value) {
    sum.add(secondsPerPeriod * value);
}

template <typename T>
const T& RateEstimator<T>::getRate() const {
    return sum.getSum();
}

// Computes the mean and standard deviation
std::pair<double, double> meanAndStandardDev(const std::vector<double> & xs);

#endif  // PBRT_CLOUD_ESTIMATORS_H
