#ifndef PBRT_CLOUD_TREELET_DUMP_BVH_H
#define PBRT_CLOUD_TREELET_DUMP_BVH_H

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

class TreeletDumpBVH : public BVHAccel {
  public:
    enum class TraversalAlgorithm {
        CheckSend,
        SendCheck
    };

    enum class PartitionAlgorithm {
        PseudoAgglomerative,
        OneByOne,
        TopologicalHierarchical,
        GreedySize
    };

    struct Edge {
        uint64_t src;
        uint64_t dst;
        float weight;

        Edge(uint64_t src, uint64_t dst, float weight)
            : src(src), dst(dst), weight(weight)
        {}
    };

    struct IntermediateTraversalGraph {
        std::deque<Edge> edges;
        std::vector<uint64_t> depthFirst;

        std::deque<std::pair<uint64_t, uint64_t>> outgoing;
    };

    struct TraversalGraph {
        std::vector<Edge> edges;
        std::vector<uint64_t> depthFirst;

        std::vector<std::pair<Edge *, uint64_t>> outgoing;
    };

    using TreeletMap = std::array<std::vector<uint32_t>, 8>;
    using RayCountMap = std::vector<std::unordered_map<uint64_t, std::atomic_uint64_t>>;

    TreeletDumpBVH(std::vector<std::shared_ptr<Primitive>> &&p,
                   int maxTreeletBytes,
                   bool rootBVH,
                   TraversalAlgorithm traversal,
                   PartitionAlgorithm partition,
                   int maxPrimsInNode = 1,
                   SplitMethod splitMethod = SplitMethod::SAH,
                   bool dumpBVH = false,
                   const std::string &dumpBVHPath = "");

    TreeletDumpBVH(std::vector<std::shared_ptr<Primitive>> &&p,
                   LinearBVHNode *deserializedNodes,
                   int deserializedNodeCount,
                   int maxTreeletBytes,
                   TraversalAlgorithm traversal,
                   PartitionAlgorithm partition);

    bool Intersect(const Ray &ray, SurfaceInteraction *isect) const;
    bool IntersectP(const Ray &ray) const;

  private:
    struct TreeletInfo {
        std::list<int> nodes {}; 
        std::unordered_set<BVHAccel *> instances {};
        uint64_t noInstanceSize {0};
        uint64_t instanceSize {0};
        int dirIdx {-1};
        float totalProb {0};
    };

    void SetNodeInfo(int maxTreeletBytes);
    std::vector<TreeletInfo> AllocateTreelets(int maxTreeletBytes);

    IntermediateTraversalGraph CreateTraversalGraphSendCheck(const Vector3f &rayDir, int depthReduction) const;

    IntermediateTraversalGraph CreateTraversalGraphCheckSend(const Vector3f &rayDir, int depthReduction) const;

    TraversalGraph CreateTraversalGraph(const Vector3f &rayDir, int depthReduction) const;

    std::vector<uint32_t>
        ComputeTreeletsAgglomerative(const TraversalGraph &graph,
                                     uint64_t maxTreeletBytes) const;

    std::vector<uint32_t>
        ComputeTreeletsTopological(const TraversalGraph &graph,
                                   uint64_t maxTreeletBytes) const;

    std::vector<uint32_t>
        ComputeTreeletsTopologicalHierarchical(const TraversalGraph &graph,
                                               uint64_t maxTreeletBytes) const;

    std::vector<uint32_t>
        ComputeTreeletsGreedySize(const TraversalGraph &graph,
                                  uint64_t maxTreeletBytes) const;

    std::vector<uint32_t> ComputeTreelets(const TraversalGraph &graph,
                                          uint64_t maxTreeletBytes) const;

    void DumpTreelets(const std::vector<TreeletInfo> &treelets) const;

    std::vector<uint32_t> OrigAssignTreelets(const uint64_t) const;

    bool IntersectSendCheck(const Ray &ray,
                            SurfaceInteraction *isect) const;
    bool IntersectPSendCheck(const Ray &ray) const;

    bool IntersectCheckSend(const Ray &ray,
                            SurfaceInteraction *isect) const;
    bool IntersectPCheckSend(const Ray &ray) const;
    std::array<RayCountMap, 8> rayCounts;
    TreeletMap treeletAllocations{};
    std::vector<uint32_t> origTreeletAllocation{};

    bool rootBVH;
    TraversalAlgorithm traversalAlgo;
    PartitionAlgorithm partitionAlgo;
    std::vector<uint64_t> nodeParents;
    std::vector<uint64_t> nodeSizes;
    std::vector<uint64_t> nodeNoInstanceSizes;
    std::unordered_map<BVHAccel *, uint64_t> instanceSizes;
    std::unordered_map<BVHAccel *, std::vector<int>> instanceInclusions;
    std::unordered_map<BVHAccel *, std::vector<int>> instanceImpacts;
    std::vector<std::vector<BVHAccel *>> nodeInstances;
    std::vector<uint64_t> subtreeSizes;
};

std::shared_ptr<TreeletDumpBVH> CreateTreeletDumpBVH(
    std::vector<std::shared_ptr<Primitive>> prims, const ParamSet &ps);

}

#endif
