#ifndef PBRT_UTIL_TIMERFD_H
#define PBRT_UTIL_TIMERFD_H

#include <sys/timerfd.h>
#include <unistd.h>
#include <chrono>
#include <cstring>

#include "util/exception.h"
#include "util/file_descriptor.h"
#include "util/util.h"

struct TimerFD {
    FileDescriptor fd;
    itimerspec timerspec;

    template <class Duration>
    TimerFD(const Duration& duration)
        : fd(CheckSystemCall("timerfd",
                             timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK))) {
        timerspec.it_interval = to_timespec(duration);
        timerspec.it_value = to_timespec(duration);
        CheckSystemCall("timerfd_settime",
                        timerfd_settime(fd.fd_num(), 0, &timerspec, nullptr));
    }

    void reset() {
        char buffer[8];
        CheckSystemCall("read", read(fd.fd_num(), buffer, 8));
    }

    ~TimerFD() {
        std::memset(&timerspec, 0, sizeof(itimerspec));
        CheckSystemCall("timerfd_settime",
                        timerfd_settime(fd.fd_num(), 0, &timerspec, nullptr));
    }
};

#endif /* PBRT_UTIL_TIMERFD_H */
