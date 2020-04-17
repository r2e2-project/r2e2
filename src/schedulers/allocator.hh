#pragma once

#include <functional>
#include <map>
#include <numeric>
#include <queue>
#include <utility>
#include <vector>

#include "common/lambda.hh"

struct AllocationInfo
{
  double targetWeight;
  uint32_t allocations;
  TreeletId id;

  AllocationInfo( double tw, uint32_t allocN, TreeletId treeletId )
    : targetWeight( tw )
    , allocations( allocN )
    , id( treeletId )
  {}
};

class Allocator
{
public:
  Allocator();

  // Modify the target allocation ratios underlying this allocator. Need not
  // be normalized.
  void setTargetWeights( std::map<TreeletId, double>&& weights );

  // Add a treelet to this allocator's pool
  void addTreelet( TreeletId id );

  TreeletId getAllocation( WorkerId id ) const;
  const std::vector<WorkerId>& getLocations( TreeletId id ) const;

  // First, allocates any unallocated treelets.
  // Then, allocates treelets with the greatest discrepency between
  // allocation and load.
  TreeletId allocate( WorkerId id );

  // Does this allocator have any unallocated treelets?
  bool anyUnassignedTreelets();

private:
  // TODO(aozdemir): Right now, we maintain a list of treelets sorted by
  // allocation priority and do a full resort on any modification to this
  // list. Some definitions of allocation priority will not require a full
  // resort. I didn't do this optimization now, because we may want to
  // experiment with allocation priority metrics.
  void resort();

  std::vector<TreeletId> unassignedTreelets;
  std::map<WorkerId, TreeletId> allocations;
  std::map<TreeletId, double> targetWeights;
  uint32_t netAllocations;
  std::vector<AllocationInfo> sortedTreelets;
  std::map<TreeletId, std::vector<WorkerId>> workersPerTreelet;
};
