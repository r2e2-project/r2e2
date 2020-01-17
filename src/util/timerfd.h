#ifndef PBRT_UTIL_TIMERFD_H
#define PBRT_UTIL_TIMERFD_H

#include <sys/timerfd.h>
#include <unistd.h>
#include <chrono>
#include <cstring>

#include "util/exception.h"
#include "util/file_descriptor.h"
#include "util/util.h"

class TimerFD : public FileDescriptor {
  private:
    itimerspec timerspec;

  public:
    TimerFD() : TimerFD(std::chrono::seconds{0}, std::chrono::seconds{0}) {}

    template <class DurationA, class DurationB>
    TimerFD(const DurationA& interval, const DurationB& initial)
        : FileDescriptor(CheckSystemCall(
              "timerfd", timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK))) {
        set(interval, initial);
    }

    template <class Duration>
    TimerFD(const Duration& interval) : TimerFD(interval, interval) {}

    template <class DurationA, class DurationB>
    void set(const DurationA& interval, const DurationB& initial) {
        timerspec.it_interval = to_timespec(interval);
        timerspec.it_value = to_timespec(initial);
        CheckSystemCall("timerfd_settime",
                        timerfd_settime(fd_num(), 0, &timerspec, nullptr));
    }

    void disarm() { set(std::chrono::seconds{0}, std::chrono::seconds{0}); }

    void read_event() { read(8); }

    ~TimerFD() { disarm(); }
};

#endif /* PBRT_UTIL_TIMERFD_H */
