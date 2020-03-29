#pragma once

#include <cstdint>
#include <set>

namespace r2t2 {

/**
 * A data structure for storing sets of unsigned integers, which is optimized to
 * for storing sets which tend to have most of their values in a contiguous
 * sequence starting at 0.
 */
class SeqNoSet
{
public:
  /**
   * Creates an empty set
   */
  bool contains( const uint64_t value ) const;
  void insert( const uint64_t value );
  uint64_t size() const;
  uint64_t numberOfItemsInMemory() const;

  void insertAllBelow( const uint64_t value );

  const std::set<uint64_t>& set() { return set_; }
  uint64_t smallest_not_in_set() { return smallest_not_in_set_; }

private:
  std::set<uint64_t> set_ {};
  uint64_t smallest_not_in_set_ { 0 };
};

} // namespace r2t2
