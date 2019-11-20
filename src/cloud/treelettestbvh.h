#ifndef PBRT_ACCELERATORS_TREELET_TEST_BVH_H
#define PBRT_ACCELERATORS_TREELET_TEST_BVH_H

#include "accelerators/bvh.h"
#include "cloud/bvh.h"
#include "pbrt.h"
#include "primitive.h"
#include <atomic>
#include <memory>
#include <set>
#include <unordered_map>
#include <vector>

namespace pbrt {

class TreeletTestBVH : public BVHAccel {
  public:
    enum class TraversalAlgorithm {
        CheckSend,
        SendCheck
    };

    enum class PartitionAlgorithm {
        PseudoAgglomerative,
        OneByOne
    };

    struct Edge {
        uint64_t src;
        uint64_t dst;
        float weight;

        Edge(uint64_t src, uint64_t dst, float weight)
            : src(src), dst(dst), weight(weight)
        {}
    };

    struct TraversalGraph {
        std::vector<Edge> edges;
        std::vector<uint64_t> topoSort;

        std::vector<std::pair<Edge *, uint64_t>> outgoing;
        std::vector<std::unordered_map<uint64_t, std::atomic_uint64_t>> rayCounts;

        TraversalGraph(int nodeCount, int maxOutgoing);
        TraversalGraph() = default;
    };

    using TreeletMap = std::array<std::vector<uint32_t>, 8>;

    TreeletTestBVH(std::vector<std::shared_ptr<Primitive>> &&p,
                   int maxTreeletBytes,
                   TraversalAlgorithm traversal,
                   PartitionAlgorithm partition,
                   int maxPrimsInNode = 1,
                   SplitMethod splitMethod = SplitMethod::SAH,
                   bool dumpBVH = false,
                   const std::string &dumpBVHPath = "");

    TreeletTestBVH(std::vector<std::shared_ptr<Primitive>> &&p,
                   LinearBVHNode *deserializedNodes,
                   int deserializedNodeCount,
                   int maxTreeletBytes,
                   TraversalAlgorithm traversal,
                   PartitionAlgorithm partition);

    bool Intersect(const Ray &ray, SurfaceInteraction *isect) const;
    bool IntersectP(const Ray &ray) const;

  private:
    void SetNodeSizes();
    void AllocateTreelets(int maxTreeletBytes);

    TraversalGraph CreateTraversalGraphSendCheck(const Vector3f &rayDir) const;

    TraversalGraph CreateTraversalGraphCheckSend(const Vector3f &rayDir) const;

    TraversalGraph CreateTraversalGraph(const Vector3f &rayDir) const;

    std::vector<uint32_t>
        ComputeTreeletsAgglomerative(const TraversalGraph &graph,
                                     uint64_t maxTreeletBytes) const;

    std::vector<uint32_t>
        ComputeTreeletsTopological(const TraversalGraph &graph,
                                   uint64_t maxTreeletBytes) const;

    std::vector<uint32_t> ComputeTreelets(const TraversalGraph &graph,
                                          uint64_t maxTreeletBytes) const;

    std::vector<uint32_t> OrigAssignTreelets(const uint64_t) const;

    uint64_t GetNodeSize(int nodeIdx) const;

    bool IntersectSendCheck(const Ray &ray,
                            SurfaceInteraction *isect) const;
    bool IntersectPSendCheck(const Ray &ray) const;

    bool IntersectCheckSend(const Ray &ray,
                            SurfaceInteraction *isect) const;
    bool IntersectPCheckSend(const Ray &ray) const;

    std::array<TraversalGraph, 8> graphs{};
    TreeletMap treeletAllocations{};
    std::vector<uint32_t> origTreeletAllocation{};

    TraversalAlgorithm traversalAlgo;
    PartitionAlgorithm partitionAlgo;
    std::vector<uint64_t> nodeSizes;
};

std::shared_ptr<TreeletTestBVH> CreateTreeletTestBVH(
    std::vector<std::shared_ptr<Primitive>> prims, const ParamSet &ps);

}

#endif
