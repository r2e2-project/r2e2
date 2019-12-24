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
    template <class DurationA, class DurationB>
    TimerFD(const DurationA& duration, const DurationB& initial)
        : FileDescriptor(CheckSystemCall(
              "timerfd", timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK))) {
        timerspec.it_interval = to_timespec(duration);
        timerspec.it_value = to_timespec(initial);
        CheckSystemCall("timerfd_settime",
                        timerfd_settime(fd_num(), 0, &timerspec, nullptr));
    }

    template <class Duration>
    TimerFD(const Duration& duration) : TimerFD(duration, duration) {}

    void reset() { read(8); }

    ~TimerFD() {
        std::memset(&timerspec, 0, sizeof(itimerspec));
        CheckSystemCall("timerfd_settime",
                        timerfd_settime(fd_num(), 0, &timerspec, nullptr));
    }
};

#endif /* PBRT_UTIL_TIMERFD_H */
