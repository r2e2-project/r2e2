#pragma once

#include <chrono>
#include <cstdint>

class CPUStats
{
public:
  CPUStats();
  CPUStats operator-( const CPUStats& other ) const;

  static const int64_t TIME_UNIT; /* ticks per second */

  uint64_t user;
  uint64_t nice;
  uint64_t system;
  uint64_t idle;
  uint64_t iowait;
  uint64_t irq;
  uint64_t soft_irq;
  uint64_t steal;
  uint64_t guest;
  uint64_t guest_nice;
};
