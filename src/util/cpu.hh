#pragma once

#include <chrono>
#include <cstdint>

class CPUStats
{
public:
  CPUStats();
  CPUStats operator-( const CPUStats& other ) const;

  static const int64_t TIME_UNIT; /* ticks per second */

  uint64_t user { 0 };
  uint64_t nice { 0 };
  uint64_t system { 0 };
  uint64_t idle { 0 };
  uint64_t iowait { 0 };
  uint64_t irq { 0 };
  uint64_t soft_irq { 0 };
  uint64_t steal { 0 };
  uint64_t guest { 0 };
  uint64_t guest_nice { 0 };
};
