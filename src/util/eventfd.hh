#ifndef PBRT_UTIL_EVENTFD_H
#define PBRT_UTIL_EVENTFD_H

#include <sys/eventfd.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <string>

#include "util/exception.hh"
#include "util/file_descriptor.hh"

class EventFD : public FileDescriptor {
  public:
    EventFD(const bool semaphore = false)
        : FileDescriptor(
              eventfd(0u, (semaphore ? EFD_SEMAPHORE : 0) | EFD_NONBLOCK)) {}

    bool read_event() {
        uint64_t value;
        int retval = ::read(fd_num(), &value, sizeof(value));

        if (retval == sizeof(value)) {
            return true;
        } else if (retval < 0 && errno == EAGAIN) {
            return false;
        } else {
            throw unix_error("eventfd_read");
        }
    }

    void write_event() {
        uint64_t value = 1;
        if (::write(fd_num(), &value, sizeof(value)) < 0) {
            throw unix_error("eventfd_write");
        }
    }
};

#endif /* PBRT_UTIL_EVENTFD_H */
