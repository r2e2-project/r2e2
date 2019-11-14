#ifndef PBRT_ACCELERATORS_TREELET_TEST_BVH_H
#define PBRT_ACCELERATORS_TREELET_TEST_BVH_H

#include "accelerators/bvh.h"
#include "cloud/bvh.h"
#include "pbrt.h"
#include "primitive.h"
#include <memory>
#include <set>
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
        unsigned dstBytes; // Cache for performance

        Edge(int src, int dst, float modelWeight, int rayCount,
             unsigned dstBytes)
            : src(src), dst(dst), modelWeight(modelWeight),
              rayCount(rayCount), dstBytes(dstBytes)
        {}
    };

    struct OutEdges {
        Edge *missEdge = nullptr;
        Edge *hitEdge = nullptr;
    };

    struct TraversalGraph {
        std::vector<OutEdges> adjacencyList;
        std::vector<Edge> edgeList;
        std::vector<int> topologicalVertices;
    };

    TraversalGraph createTraversalGraph(const Vector3f &rayDir) const;

    std::vector<uint32_t>
        computeTreeletsAgglomerative(const TraversalGraph &graph,
                                     int maxTreeletBytes) const;

    std::vector<uint32_t>
        computeTreeletsTopological(const TraversalGraph &graph,
                                   int maxTreeletBytes) const;

    std::vector<uint32_t> computeTreelets(const TraversalGraph &graph,
                                          int maxTreeletBytes) const;

    unsigned getNodeSize(int nodeIdx) const;

    TreeletMap treeletAllocations{};
    const unsigned nodeSize = sizeof(CloudBVH::TreeletNode);
    // Assume on average 2 unique vertices, normals etc per triangle
    const unsigned leafSize = 3 * sizeof(int) + 2 * (sizeof(Point3f) +
            sizeof(Normal3f) + sizeof(Vector3f) + sizeof(Point2f));
};

std::shared_ptr<TreeletTestBVH> CreateTreeletTestBVH(
    std::vector<std::shared_ptr<Primitive>> prims, const ParamSet &ps);

}

#endif
