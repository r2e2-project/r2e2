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

    bool Intersect(const Ray &ray, SurfaceInteraction *isect) const;
    bool IntersectP(const Ray &ray) const;

  private:
    struct Edge {
        uint64_t src;
        uint64_t dst;
        float modelWeight;
        uint64_t rayCount;
        uint64_t dstBytes; // Cache for performance

        Edge(uint64_t src, uint64_t dst, float modelWeight, uint64_t rayCount,
             uint64_t dstBytes)
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
        std::vector<uint64_t> topologicalVertices;
    };

    TraversalGraph createTraversalGraph(const Vector3f &rayDir) const;

    std::vector<uint32_t>
        computeTreeletsAgglomerative(const TraversalGraph &graph,
                                     uint64_t maxTreeletBytes) const;

    std::vector<uint32_t>
        computeTreeletsTopological(const TraversalGraph &graph,
                                   uint64_t maxTreeletBytes) const;

    std::vector<uint32_t> computeTreelets(const TraversalGraph &graph,
                                          uint64_t maxTreeletBytes) const;

    std::vector<uint32_t> origAssignTreelets(const uint64_t) const;

    uint64_t getNodeSize(int nodeIdx) const;

    std::array<TraversalGraph, 8> graphs;
    TreeletMap treeletAllocations{};
    std::vector<uint32_t> origTreeletAllocation;
};

std::shared_ptr<TreeletTestBVH> CreateTreeletTestBVH(
    std::vector<std::shared_ptr<Primitive>> prims, const ParamSet &ps);

}

#endif
