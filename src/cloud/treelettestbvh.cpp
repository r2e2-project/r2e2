#include "cloud/treelettestbvh.h"

namespace pbrt {

TreeletTestBVH::TreeletTestBVH(std::vector<std::shared_ptr<Primitive>> p,
                               int maxPrimsInNode, SplitMethod splitMethod)
    : BVHAccel(p, maxPrimsInNode, splitMethod) {
    for (int i = 0; i < 8; i++) {
        auto weights = assignWeights(i);
        treeletAllocations[i] = computeTreelets(weights);
    }
}

TreeletTestBVH::TreeletTestBVH(std::vector<std::shared_ptr<Primitive>> p,
                               int maxPrimsInNode, SplitMethod splitMethod,
                               TreeletTestBVH::TreeletMap &&treelets)
    : BVHAccel(p, maxPrimsInNode, splitMethod),
      treeletAllocations(std::move(treelets))
{}

std::shared_ptr<TreeletTestBVH> CreateTreeletTestBVH(
    std::vector<std::shared_ptr<Primitive>> prims, const ParamSet &ps) {
    std::string splitMethodName = ps.FindOneString("splitmethod", "sah");
    BVHAccel::SplitMethod splitMethod;
    if (splitMethodName == "sah")
        splitMethod = BVHAccel::SplitMethod::SAH;
    else if (splitMethodName == "hlbvh")
        splitMethod = BVHAccel::SplitMethod::HLBVH;
    else if (splitMethodName == "middle")
        splitMethod = BVHAccel::SplitMethod::Middle;
    else if (splitMethodName == "equal")
        splitMethod = BVHAccel::SplitMethod::EqualCounts;
    else {
        Warning("BVH split method \"%s\" unknown.  Using \"sah\".",
                splitMethodName.c_str());
        splitMethod = BVHAccel::SplitMethod::SAH;
    }

    int maxPrimsInNode = ps.FindOneInt("maxnodeprims", 4);
    auto res = std::make_shared<TreeletTestBVH>(std::move(prims),
                                                maxPrimsInNode, splitMethod);

    return res;
}

std::vector<std::vector<std::pair<int, double>>>
TreeletTestBVH::assignWeights(int idx) const {
    std::vector<std::vector<std::pair<int, double>>> weights;



    return weights;
}

std::unordered_map<int, int> TreeletTestBVH::computeTreelets(
    const std::vector<std::vector<std::pair<int, double>>> &weights) const {

}

}
