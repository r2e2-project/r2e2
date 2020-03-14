#include "allocator.h"

#include <glog/logging.h>

#include <algorithm>
#include <iomanip>
#include <iostream>

using namespace std;

Allocator::Allocator()
    : unassignedTreelets{},
      netAllocations{0},
      allocations{},
      targetWeights{},
      sortedTreelets{},
      workersPerTreelet{} {}

void Allocator::setTargetWeights(std::map<TreeletId, double>&& weights) {
    targetWeights = move(weights);

    double sum = std::accumulate(
        targetWeights.begin(), targetWeights.end(), 0.0,
        [](double a, std::map<TreeletId, double>::const_reference kv) {
            return a + kv.second;
        });

    for (auto& kv : targetWeights) {
        kv.second /= sum;
    }

    for (auto& i : sortedTreelets) {
        i.targetWeight = targetWeights[i.id];
    }

    resort();
}

// Our function for determining how much surplus a treelet has
double surplus(const AllocationInfo& allocation, uint32_t netAllocations) {
    if (netAllocations == 0) {
        return 1.0;
    } else if (allocation.targetWeight == 0.0) {
        return 1000.0;
    } else {
        return double(allocation.allocations) / double(netAllocations) /
               allocation.targetWeight;
    }
}

void Allocator::resort() {
    sort(sortedTreelets.begin(), sortedTreelets.end(),
         [netAllocations = this->netAllocations](const AllocationInfo& x,
                                                 const AllocationInfo& y) {
             double xSurplus = surplus(x, netAllocations);
             double ySurplus = surplus(y, netAllocations);
             return xSurplus != ySurplus ? xSurplus < ySurplus : x.id < y.id;
         });
}

void Allocator::addTreelet(TreeletId id) { unassignedTreelets.push_back(id); }

TreeletId Allocator::getAllocation(WorkerId id) const {
    return allocations.at(id);
}

const std::vector<WorkerId>& Allocator::getLocations(TreeletId id) const {
    return workersPerTreelet.at(id);
}

TreeletId Allocator::allocate(WorkerId wid) {
    TreeletId tid;
    netAllocations += 1;
    if (unassignedTreelets.empty()) {
        AllocationInfo& i{sortedTreelets.front()};
        i.allocations += 1;
        tid = i.id;
    } else {
        tid = unassignedTreelets.back();
        unassignedTreelets.pop_back();
        sortedTreelets.push_back(AllocationInfo{targetWeights[tid], 1, tid});
    }
    resort();
    workersPerTreelet[tid].push_back(wid);
    allocations[wid] = tid;
    return tid;
}

bool Allocator::anyUnassignedTreelets() { return !unassignedTreelets.empty(); }
