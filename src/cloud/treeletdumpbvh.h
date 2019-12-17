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

struct InstanceMask {
    static constexpr int numInts = 5;
    uint64_t mask[numInts] {};

    InstanceMask & operator|=(const InstanceMask &o) {
        for (int i = 0; i < numInts; i++) {
            mask[i] |= o.mask[i];
        }

        return *this;
    }
    
    friend InstanceMask operator|(InstanceMask l, const InstanceMask &r) {
        for (int i = 0; i < numInts; i++) {
            l.mask[i] |= r.mask[i];
        }

        return l;
    }

    friend bool operator==(const InstanceMask &l, const InstanceMask &r) {
        for (int i = 0; i < numInts; i++) {
            if (l.mask[i] != r.mask[i]) {
                return false;
            }
        }

        return true;
    }

    bool Get(int idx) const {
        int intIdx = idx >> 6;
        int bitIdx = idx & 63;

        return mask[intIdx] & (1UL << bitIdx);
    }

    void Set(int idx) {
        int intIdx = idx >> 6;
        int bitIdx = idx & 63;

        mask[intIdx] |= (1UL << bitIdx);
    }
};

}

namespace std {
template <>
struct hash<pbrt::InstanceMask>
{
    size_t operator()(const pbrt::InstanceMask &mask) const {
        size_t hash = 0;
        for (int i = 0; i < pbrt::InstanceMask::numInts; i++) {
            hash ^= std::hash<uint64_t>()(mask.mask[i]);
        }
        return hash;
    }
};
}


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
        GreedySize,
        Nvidia,
        MergedGraph
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
        std::vector<float> incomingProb;

        std::deque<std::pair<uint64_t, uint64_t>> outgoing;
    };

    struct TraversalGraph {
        std::vector<Edge> edges;
        std::vector<uint64_t> depthFirst;
        std::vector<float> incomingProb;

        std::vector<std::pair<Edge *, uint64_t>> outgoing;
    };

    using TreeletMap = std::array<std::vector<uint32_t>, 8>;
    using RayCountMap = std::vector<std::unordered_map<uint64_t, std::atomic_uint64_t>>;

    TreeletDumpBVH(std::vector<std::shared_ptr<Primitive>> &&p,
                   int maxTreeletBytes,
                   int copyableThreshold,
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
                   int copyableThreshold,
                   TraversalAlgorithm traversal,
                   PartitionAlgorithm partition);

    bool Intersect(const Ray &ray, SurfaceInteraction *isect) const;
    bool IntersectP(const Ray &ray) const;

  private:
    struct TreeletInfo {
        std::list<int> nodes {}; 
        InstanceMask instanceMask;
        std::vector<TreeletDumpBVH *> instances;
        uint64_t noInstanceSize {0};
        uint64_t instanceSize {0};
        int dirIdx {-1};
        float totalProb {0};
    };

    void SetNodeInfo(int maxTreeletBytes);

    uint64_t GetInstancesBytes(const InstanceMask &mask) const;

    std::unordered_map<uint32_t, TreeletInfo> MergeDisjointTreelets(int dirIdx, int maxTreeletBytes, const TraversalGraph &graph);
    void OrderTreeletNodesDepthFirst(int numDirs, std::vector<TreeletInfo> &treelets);

    std::vector<TreeletInfo> AllocateUnspecializedTreelets(int maxTreeletBytes);
    std::vector<TreeletInfo> AllocateDirectionalTreelets(int maxTreeletBytes);
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

    std::vector<uint32_t> DumpTreelets(bool root) const;

    std::vector<uint32_t> OrigAssignTreelets(const uint64_t) const;

    bool IntersectSendCheck(const Ray &ray,
                            SurfaceInteraction *isect) const;
    bool IntersectPSendCheck(const Ray &ray) const;

    bool IntersectCheckSend(const Ray &ray,
                            SurfaceInteraction *isect) const;
    bool IntersectPCheckSend(const Ray &ray) const;
    std::array<RayCountMap, 8> rayCounts;
    TreeletMap treeletAllocations{};

    bool rootBVH;
    TraversalAlgorithm traversalAlgo;
    PartitionAlgorithm partitionAlgo;
    std::vector<uint64_t> nodeParents;
    std::vector<uint64_t> nodeSizes;
    std::vector<uint64_t> subtreeSizes;

    std::vector<TreeletInfo> allTreelets;

    uint64_t totalBytes {0};
    std::vector<InstanceMask> nodeInstanceMasks {};
    std::vector<InstanceMask> subtreeInstanceMasks {};
    std::array<TreeletDumpBVH *, sizeof(InstanceMask) * 8> uniqueInstances {};
    std::array<uint64_t, sizeof(InstanceMask) * 8> instanceSizes {};
    std::array<std::array<float, sizeof(InstanceMask) * 8>, 8> instanceProbabilities {};

    static int numInstances;
    int instanceID = 0;
    bool copyable = false;

    std::unordered_map<InstanceMask, uint64_t> instanceSizeCache;
};

std::shared_ptr<TreeletDumpBVH> CreateTreeletDumpBVH(
    std::vector<std::shared_ptr<Primitive>> prims, const ParamSet &ps);

}

#endif
