#pragma once

#include <sys/timerfd.h>
#include <unistd.h>

#include <chrono>
#include <cstring>

#include "util/exception.hh"
#include "util/file_descriptor.hh"
#include "util/util.hh"

class TimerFD : public FileDescriptor {
  private:
    itimerspec timerspec_;
    bool armed_{false};
    bool recurring_{false};

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
        to_timespec(interval, timerspec_.it_interval);
        to_timespec(initial, timerspec_.it_value);
        CheckSystemCall("timerfd_settime",
                        timerfd_settime(fd_num(), 0, &timerspec_, nullptr));

        armed_ = (initial != std::chrono::nanoseconds::zero());
        recurring_ = (interval != std::chrono::nanoseconds::zero());
    }

    void disarm() { set(std::chrono::seconds{0}, std::chrono::seconds{0}); }

    bool armed() const { return armed_; }
    bool recurring() const { return recurring_; }

    void read_event() {
        char buffer[8];
        std::string_view sv(buffer, sizeof(buffer));
        read(sv);

        if (!recurring_) armed_ = false;
    }

    ~TimerFD() { disarm(); }
};
