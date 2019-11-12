#ifndef PBRT_ACCELERATORS_TREELET_TEST_BVH_H
#define PBRT_ACCELERATORS_TREELET_TEST_BVH_H

#include "accelerators/bvh.h"
#include "pbrt.h"
#include "primitive.h"
#include <unordered_map>
#include <memory>
#include <vector>

namespace pbrt {

class TreeletTestBVH : public BVHAccel {
  public:
    using TreeletMap = std::array<std::vector<uint32_t>, 8>;

    TreeletTestBVH(std::vector<std::shared_ptr<Primitive>> p,
                   int maxPrimsInNode = 1,
                   SplitMethod splitMethod = SplitMethod::SAH);

    TreeletTestBVH(std::vector<std::shared_ptr<Primitive>> p,
                   TreeletMap &&treelets,
                   int maxPrimsInNode = 1,
                   SplitMethod splitMethod = SplitMethod::SAH);
  private:
    std::vector<std::vector<std::pair<int, double>>> assignWeights(
        const Vector3f &rayDir) const;
    std::vector<uint32_t> computeTreelets( 
        const std::vector<std::vector<std::pair<int, double>>> &weights) const;

    TreeletMap treeletAllocations{};
};

std::shared_ptr<TreeletTestBVH> CreateTreeletTestBVH(
    std::vector<std::shared_ptr<Primitive>> prims, const ParamSet &ps);

}

#endif
