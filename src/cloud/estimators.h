#include <chrono>
#include <math.h>

/**
 * This class handles a stream of incoming (value, time) pairs, where the time
 * values are monotonically increasing, but not uniformly distributed, and
 * maintains an estimate of the average value over the last `period`.
 *
 * An implementation of the (linearly interpolated) exponential moving average
 * as described in S4.3 of this paper:
 *   http://www.eckner.com/papers/Algorithms%20for%20Unevenly%20Spaced%20Time%20Series.pdf
 * That paper cites a high-frequency trading book written by Muller.
 *
 * T must support addition and multiplication by doubles
 */
template <typename T>
class ExponentialMovingAverage {
 public:
  ExponentialMovingAverage(std::chrono::high_resolution_clock::duration period);

  // Feed this (value, time) pair into the stream.
  void update(const T& value, std::chrono::high_resolution_clock::time_point time);
  // Feed this value into the stream with the current time.
  void updateNow(const T& value);

  // Get the current average estimate
  const T& getAverage() const { return average; }

 private:
  // The period over which the moving average is computed.
  // (e.g. the average of the last `period`)
  const std::chrono::high_resolution_clock::duration period;

  // Whether the stream has seen no data yet.
  bool empty;
  // The last value fed into the stream
  T lastValue;
  // The time that the last value was fed into the stream
  std::chrono::high_resolution_clock::time_point lastTime;
  // The last value of W2, used when multiple points have the same time.
  double lastW2;

  // The current estimate of the moving average
  T average;
};

template <typename T>
ExponentialMovingAverage<T>::ExponentialMovingAverage(
    std::chrono::high_resolution_clock::duration period)
    : period(period), empty(true) {
    // lastValue, lastTime, and average will all be ignored since `empty` is
    // true.
}

template <typename T>
void ExponentialMovingAverage<T>::updateNow( const T& value ) {
  update( value, std::chrono::high_resolution_clock::now() );
}

template <typename T>
void ExponentialMovingAverage<T>::update( const T& value, std::chrono::high_resolution_clock::time_point time ) {
  if ( empty ) {
    empty = false;
    average = value;
    lastW2 = 1.0;
    lastValue = value;
  } else {
    std::chrono::high_resolution_clock::duration deltaT = time - lastTime;
    if ( deltaT <= std::chrono::nanoseconds{0} ) {
      average = average + (1.0 - lastW2) * value;
      lastValue = lastValue + value;
    } else {
        double w1 = exp(-double(deltaT.count()) / double(period.count()));
        double w2 =
            (1.0 - w1) * double(period.count()) / double(deltaT.count());
        average = w1 * average + (1.0 - w2) * value + (w2 - w1) * lastValue;
        lastW2 = w2;
        lastValue = value;
    }
  }
  lastTime = time;
}

template <typename T>
class RateEstimator {
 public:
  RateEstimator();
  void update(const T& value);
  const T& getRate();
 private:
  bool empty;
  std::chrono::high_resolution_clock::time_point lastTime;
  ExponentialMovingAverage<T> averager;
};

template <typename T>
RateEstimator<T>::RateEstimator() : empty(true), averager(std::chrono::seconds(1)) {}

template <typename T>
void RateEstimator<T>::update(const T& value) {
  std::chrono::high_resolution_clock::time_point now = std::chrono::high_resolution_clock::now();
  if ( empty ) {
    empty = false;
  } else {
    std::chrono::duration<double> period = now - lastTime;
    T thisRate = value * (1.0 / period.count());
    averager.update(thisRate, now);
  }
  lastTime = now;
}

template <typename T>
const T& RateEstimator<T>::getRate() {
  return averager.getAverage();
}

