#ifndef PBRT_ACCELERATORS_TREELET_TEST_BVH_H
#define PBRT_ACCELERATORS_TREELET_TEST_BVH_H

#include "accelerators/bvh.h"
#include "pbrt.h"
#include "primitive.h"
#include <memory>
#include <unordered_map>
#include <vector>

namespace pbrt {

class TreeletTestBVH : public BVHAccel {
  public:
    using TreeletMap = std::array<std::vector<uint32_t>, 8>;

    TreeletTestBVH(std::vector<std::shared_ptr<Primitive>> p,
                   int maxTreeletBytes,
                   int maxPrimsInNode = 1,
                   SplitMethod splitMethod = SplitMethod::SAH);

    TreeletTestBVH(std::vector<std::shared_ptr<Primitive>> p,
                   TreeletMap &&treelets,
                   int maxPrimsInNode = 1,
                   SplitMethod splitMethod = SplitMethod::SAH);
  private:
    struct Edge {
        int src;
        int dst;
        float modelWeight;
        int rayCount;

        Edge(int src, int dst, float modelWeight, int rayCount)
            : src(src), dst(dst), modelWeight(modelWeight), rayCount(rayCount)
        {}
    };

    struct TraversalGraph {
        std::vector<std::unordered_map<int, Edge>> adjacencyList;
        std::vector<Edge *> sortedEdges;
        std::vector<int> topologicalVertices;
    };

    TraversalGraph createTraversalGraph(const Vector3f &rayDir) const;

    std::vector<uint32_t>
        computeTreeletsAgglomerative(const TraversalGraph &graph,
                                     int maxTreeletBytes);

    std::vector<uint32_t>
        computeTreeletsTopological(const TraversalGraph &graph,
                                   int maxTreeletBytes);

    std::vector<uint32_t> computeTreelets(const TraversalGraph &graph,
                                          int maxTreeletBytes);

    TreeletMap treeletAllocations{};
};

std::shared_ptr<TreeletTestBVH> CreateTreeletTestBVH(
    std::vector<std::shared_ptr<Primitive>> prims, const ParamSet &ps);

}

#endif
