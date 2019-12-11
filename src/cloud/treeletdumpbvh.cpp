#include "cloud/treeletdumpbvh.h"
#include "paramset.h"
#include "stats.h"
#include <algorithm>
#include <fstream>

#include "messages/utils.h"
#include "pbrt.pb.h"

using namespace std;

namespace pbrt {

STAT_COUNTER("BVH/Total Ray Transfers (new)", totalNewRayTransfers);
STAT_COUNTER("BVH/Total Ray Transfers (old)", totalOldRayTransfers);

namespace SizeEstimates {
    constexpr uint64_t nodeSize = sizeof(CloudBVH::TreeletNode);
    // triNum, faceIndex, pointer to mesh, 3 indices for triangle
    // assume on average 2 unique vertices, normals etc per triangle
    constexpr uint64_t triSize = sizeof(int) + sizeof(int) + sizeof(uintptr_t) + 
        3 * sizeof(int) + 2 * (sizeof(Point3f) + sizeof(Normal3f) +
        sizeof(Vector3f) + sizeof(Point2f));
    constexpr uint64_t instSize = 32 * sizeof(float) + sizeof(int);
}

Vector3f computeRayDir(unsigned idx) {
    unsigned x = idx & (1 << 0);
    unsigned y = idx & (1 << 1);
    unsigned z = idx & (1 << 2);

    return Vector3f(x ? 1 : -1, y ? 1 : -1, z ? 1 : -1);
}

unsigned computeIdx(const Vector3f &dir) {
    return (dir.x >= 0 ? 1 : 0) +
        ((dir.y >= 0 ? 1 : 0) << 1) +
        ((dir.z >= 0 ? 1 : 0) << 2);
}


int TreeletDumpBVH::numInstances = 0;

TreeletDumpBVH::TreeletDumpBVH(vector<shared_ptr<Primitive>> &&p,
                               int maxTreeletBytes,
                               bool rootBVH,
                               TreeletDumpBVH::TraversalAlgorithm travAlgo,
                               TreeletDumpBVH::PartitionAlgorithm partAlgo,
                               int maxPrimsInNode,
                               SplitMethod splitMethod,
                               bool dumpBVH,
                               const string &dumpBVHPath)
        : BVHAccel(p, maxPrimsInNode, splitMethod),
          rootBVH(rootBVH),
          traversalAlgo(travAlgo),
          partitionAlgo(partAlgo)
{
    if (dumpBVH) {
        ofstream file(dumpBVHPath);
        file.write((char *)&nodeCount, sizeof(int));
        file.write((char *)nodes, nodeCount * sizeof(LinearBVHNode));
        file.close();
    }

    if (rootBVH) {
        SetNodeInfo(maxTreeletBytes);
        auto treelets = AllocateTreelets(maxTreeletBytes);
        if (PbrtOptions.dumpScene) {
            DumpTreelets(treelets);
        }
    } else {
        instanceID = numInstances++;

        for (int nodeIdx = 0; nodeIdx < nodeCount; nodeIdx++) {
            const LinearBVHNode &node = nodes[nodeIdx];
            totalBytes += SizeEstimates::nodeSize + node.nPrimitives * SizeEstimates::triSize;
        }
    }
}

TreeletDumpBVH::TreeletDumpBVH(vector<shared_ptr<Primitive>> &&p,
                               LinearBVHNode *deserializedNodes,
                               int deserializedNodeCount,
                               int maxTreeletBytes,
                               TraversalAlgorithm travAlgo,
                               PartitionAlgorithm partAlgo)
    : BVHAccel(move(p), deserializedNodes, deserializedNodeCount),
      rootBVH(false),
      traversalAlgo(travAlgo),
      partitionAlgo(partAlgo)
{
    throw runtime_error("Unimplemented");
}

shared_ptr<TreeletDumpBVH> CreateTreeletDumpBVH(
    vector<shared_ptr<Primitive>> prims, const ParamSet &ps) {
    int maxTreeletBytes = ps.FindOneInt("maxtreeletbytes", 1'000'000'000);

    string travAlgoName = ps.FindOneString("traversal", "sendcheck");
    TreeletDumpBVH::TraversalAlgorithm travAlgo;
    if (travAlgoName == "sendcheck")
        travAlgo = TreeletDumpBVH::TraversalAlgorithm::SendCheck;
    else if (travAlgoName == "checksend")
        travAlgo = TreeletDumpBVH::TraversalAlgorithm::CheckSend;
    else {
        Warning("BVH traversal algorithm \"%s\" unknown. Using \"SendCheck\".",
                travAlgoName.c_str());
    }

    string partAlgoName = ps.FindOneString("partition", "onebyone");
    TreeletDumpBVH::PartitionAlgorithm partAlgo;
    if (partAlgoName == "onebyone")
        partAlgo = TreeletDumpBVH::PartitionAlgorithm::OneByOne;
    else if (partAlgoName == "topohierarchical")
        partAlgo = TreeletDumpBVH::PartitionAlgorithm::TopologicalHierarchical;
    else if (partAlgoName == "greedysize")
        partAlgo = TreeletDumpBVH::PartitionAlgorithm::GreedySize;
    else if (partAlgoName == "agglomerative")
        partAlgo = TreeletDumpBVH::PartitionAlgorithm::PseudoAgglomerative;
    else {
        Warning("BVH partition algorithm \"%s\" unknown. Using \"OneByOne\".",
                partAlgoName.c_str());
    }

    bool rootBVH = ps.FindOneBool("sceneaccelerator", false);

    string serializedBVHPath = ps.FindOneString("bvhnodes", "");
    if (serializedBVHPath != "") {
        ifstream bvhfile(serializedBVHPath);
        int nodeCount;
        bvhfile.read((char *)&nodeCount, sizeof(int));
        LinearBVHNode *nodes = AllocAligned<LinearBVHNode>(nodeCount);
        bvhfile.read((char *)nodes, nodeCount * sizeof(LinearBVHNode));
        bvhfile.close();

        CHECK_EQ(true, false);

        return make_shared<TreeletDumpBVH>(move(prims), nodes, nodeCount,
                                           maxTreeletBytes,
                                           travAlgo, partAlgo);
    } else {
        string splitMethodName = ps.FindOneString("splitmethod", "sah");
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

        string dumpBVHPath = ps.FindOneString("dumpbvh", "");
        if (dumpBVHPath != "") {
            return make_shared<TreeletDumpBVH>(move(prims), maxTreeletBytes, rootBVH,
                                               travAlgo, partAlgo,
                                               maxPrimsInNode, splitMethod,
                                               true, dumpBVHPath);
        } else {
            return make_shared<TreeletDumpBVH>(move(prims), maxTreeletBytes, rootBVH,
                                               travAlgo, partAlgo,
                                               maxPrimsInNode, splitMethod);
        }
    }
}

void TreeletDumpBVH::SetNodeInfo(int maxTreeletBytes) {
    printf("Building general BVH node information\n");
    nodeSizes.resize(nodeCount);
    subtreeSizes.resize(nodeCount);
    nodeParents.resize(nodeCount);
    nodeInstanceMasks.resize(nodeCount);
    static_assert(sizeof(InstanceMask) == sizeof(uint64_t)*InstanceMask::numInts);
    CHECK_LE(numInstances, instanceSizes.size());

    for (int nodeIdx = 0; nodeIdx < nodeCount; nodeIdx++) {
        const LinearBVHNode &node = nodes[nodeIdx];

        uint64_t totalSize = SizeEstimates::nodeSize;

        for (int primIdx = 0; primIdx < node.nPrimitives; primIdx++) {
            auto &prim  = primitives[node.primitivesOffset + primIdx];
            if (prim->GetType() == PrimitiveType::Geometric) {
                totalSize += SizeEstimates::triSize;
            } else if (prim->GetType() == PrimitiveType::Transformed) {
                totalSize += SizeEstimates::instSize;

                shared_ptr<TransformedPrimitive> tp = dynamic_pointer_cast<TransformedPrimitive>(prim);
                shared_ptr<TreeletDumpBVH> instance = dynamic_pointer_cast<TreeletDumpBVH>(tp->GetPrimitive());

                uniqueInstances[instance->instanceID] = instance.get();
                instanceSizes[instance->instanceID] = instance->totalBytes;

                nodeInstanceMasks[nodeIdx].Set(instance->instanceID);
            }
        }

        nodeSizes[nodeIdx] = totalSize;
        subtreeSizes[nodeIdx] = totalSize;

        if (node.nPrimitives == 0) {
            nodeParents[nodeIdx + 1] = nodeIdx;
            nodeParents[node.secondChildOffset] = nodeIdx;
        }
    }

    for (int nodeIdx = nodeCount - 1; nodeIdx >= 0; nodeIdx--) {
        const LinearBVHNode &node = nodes[nodeIdx];
        subtreeSizes[nodeIdx] += subtreeSizes[nodeIdx + 1] +
                                 subtreeSizes[node.secondChildOffset];
    }

    printf("Done building general BVH node information\n");
}

uint64_t TreeletDumpBVH::GetInstancesBytes(const InstanceMask &mask) {
    auto iter = instanceSizeCache.find(mask);
    if (iter != instanceSizeCache.end()) {
        return iter->second;
    }

    uint64_t totalInstanceSize = 0;
    for (int instanceIdx = 0; instanceIdx < numInstances; instanceIdx++) {
        if (mask.Get(instanceIdx)) {
            totalInstanceSize += instanceSizes[instanceIdx];
        }
    }

    instanceSizeCache.emplace(mask, totalInstanceSize);

    return totalInstanceSize;
}

vector<TreeletDumpBVH::TreeletInfo> TreeletDumpBVH::AllocateTreelets(int maxTreeletBytes) {
    origTreeletAllocation = OrigAssignTreelets(maxTreeletBytes);
    for (int dirIdx = 0; dirIdx < 8; dirIdx++) {
        Vector3f dir = computeRayDir(dirIdx);
        TraversalGraph graph = CreateTraversalGraph(dir, 0);

        //rayCounts[i].resize(nodeCount);
        //// Init rayCounts so unordered_map isn't modified during intersection
        //for (uint64_t srcIdx = 0; srcIdx < nodeCount; srcIdx++) {
        //    auto outgoing = graph.outgoing[srcIdx];
        //    for (const Edge *outgoingEdge = outgoing.first;
        //         outgoingEdge < outgoing.first + outgoing.second;
        //         outgoingEdge++) {
        //        uint64_t dstIdx = outgoingEdge->dst;
        //        auto res = rayCounts[i][srcIdx].emplace(dstIdx, 0);
        //        CHECK_EQ(res.second, true);
        //    }
        //}

        treeletAllocations[dirIdx] = ComputeTreelets(graph, maxTreeletBytes);
    }

    array<unordered_map<uint32_t, TreeletInfo>, 8> intermediateTreelets;

    for (int dirIdx = 0; dirIdx < 8; dirIdx++) {
        unordered_map<uint32_t, TreeletInfo> treelets;
        for (int nodeIdx = 0; nodeIdx < nodeCount; nodeIdx++) {
            int curTreelet = treeletAllocations[dirIdx][nodeIdx];
            TreeletInfo &treelet = treelets[curTreelet];
            treelet.dirIdx = dirIdx;
            treelet.nodes.push_back(nodeIdx);
            treelet.noInstanceSize += nodeSizes[nodeIdx];
            const LinearBVHNode &node = nodes[nodeIdx];

            for (int primIdx = 0; primIdx < node.nPrimitives; primIdx++) {
                auto &prim = primitives[node.primitivesOffset + primIdx];
                if (prim->GetType() == PrimitiveType::Transformed) {
                    shared_ptr<TransformedPrimitive> tp = dynamic_pointer_cast<TransformedPrimitive>(prim);
                    shared_ptr<TreeletDumpBVH> instance = dynamic_pointer_cast<TreeletDumpBVH>(tp->GetPrimitive());

                    CHECK_NOTNULL(instance.get());
                    if (!treelet.instanceMask.Get(instance->instanceID)) {
                        treelet.instanceMask.Set(instance->instanceID);
                        treelet.instanceSize += instance->totalBytes;
                    }
                }
            }
        }

        struct TreeletSortKey {
            uint32_t treeletID;
            uint64_t treeletSize;

            TreeletSortKey(uint32_t treeletID, uint64_t treeletSize)
                : treeletID(treeletID), treeletSize(treeletSize)
            {}
        };

        struct TreeletCmp {
            bool operator()(const TreeletSortKey &a, const TreeletSortKey &b) const {
                if (a.treeletSize < b.treeletSize) {
                    return true;
                }

                if (a.treeletSize > b.treeletSize) {
                    return false;
                }

                return a.treeletID < b.treeletID;
            }
        };

        map<TreeletSortKey, TreeletInfo, TreeletCmp> sortedTreelets;
        for (auto &kv : treelets) {
            CHECK_NE(kv.first, 0);
            CHECK_LT(kv.second.noInstanceSize + kv.second.instanceSize, maxTreeletBytes);
            sortedTreelets.emplace(piecewise_construct,
                    forward_as_tuple(kv.first, kv.second.noInstanceSize + kv.second.instanceSize),
                    forward_as_tuple(move(kv.second)));
        }

        // Merge treelets together
        unordered_map<uint32_t, TreeletInfo> &mergedTreelets = intermediateTreelets[dirIdx];

        auto iter = sortedTreelets.begin();
        while (iter != sortedTreelets.end()) {
            TreeletInfo &info = iter->second;

            auto candidateIter = next(iter);
            while (candidateIter != sortedTreelets.end()) {
                auto nextCandidateIter = next(candidateIter);
                TreeletInfo &candidateInfo = candidateIter->second;

                uint64_t noInstSize = info.noInstanceSize + candidateInfo.noInstanceSize;
                if (noInstSize > maxTreeletBytes) {
                    candidateIter = nextCandidateIter;
                    continue;
                }

                InstanceMask mergedMask = info.instanceMask | candidateInfo.instanceMask;
                uint64_t unionInstanceSize = GetInstancesBytes(mergedMask);

                uint64_t totalSize = noInstSize + unionInstanceSize;
                if (totalSize <= maxTreeletBytes) {
                    info.nodes.splice(info.nodes.end(), move(candidateInfo.nodes));
                    info.instanceMask = mergedMask;
                    info.instanceSize = unionInstanceSize;
                    info.noInstanceSize = noInstSize;
                    sortedTreelets.erase(candidateIter);
                }

                // No point searching further
                if (totalSize >= maxTreeletBytes - SizeEstimates::nodeSize) {
                    break;
                }

                candidateIter = nextCandidateIter;
            }

            auto nextIter = next(iter);

            mergedTreelets.emplace(iter->first.treeletID, move(info));

            sortedTreelets.erase(iter);

            iter = nextIter;
        }

        // Make final instance lists
        for (auto iter = mergedTreelets.begin(); iter != mergedTreelets.end(); iter++) {
            TreeletInfo &info = iter->second;
            for (int instanceIdx = 0; instanceIdx < numInstances; instanceIdx++) {
                if (info.instanceMask.Get(instanceIdx)) {
                    info.instances.push_back(uniqueInstances[instanceIdx]);
                }
            }
        }
    }

    vector<TreeletInfo> finalTreelets;
    // Assign root treelets to IDs 0 to 8
    for (int dirIdx = 0; dirIdx < 8; dirIdx++) {
        // Search linearly for treelet that holds node 0
        for (auto iter = intermediateTreelets[dirIdx].begin(); iter != intermediateTreelets[dirIdx].end(); iter++) {
            TreeletInfo &info = iter->second;
            bool rootFound = false;
            for (int nodeIdx : info.nodes) {
                if (nodeIdx == 0) {
                    finalTreelets.push_back(move(info));
                    intermediateTreelets[dirIdx].erase(iter);
                    rootFound = true;
                    break;
                }
            }
            if (rootFound) break;
        }
    }

    // Assign the rest contiguously
    for (int dirIdx = 0; dirIdx < 8; dirIdx++) {
        for (auto &p : intermediateTreelets[dirIdx]) {
            TreeletInfo &treelet = p.second;
            finalTreelets.push_back(move(treelet));
        }
    }

    for (int treeletID = 0; treeletID < finalTreelets.size(); treeletID++) {
        const TreeletInfo &treelet = finalTreelets[treeletID];
        for (int nodeIdx : treelet.nodes) {
            treeletAllocations[treelet.dirIdx][nodeIdx] = treeletID;
        }
    }

    return finalTreelets;
}

TreeletDumpBVH::IntermediateTraversalGraph
TreeletDumpBVH::CreateTraversalGraphSendCheck(const Vector3f &rayDir, int depthReduction) const {
    (void)depthReduction;
    IntermediateTraversalGraph g;
    g.depthFirst.reserve(nodeCount);
    g.outgoing.resize(nodeCount);

    vector<float> probabilities(nodeCount);

    bool dirIsNeg[3] = { rayDir.x < 0, rayDir.y < 0, rayDir.z < 0 };

    auto addEdge = [this, &probabilities, &g](auto src, auto dst, auto prob) {
        g.edges.emplace_back(src, dst, prob);

        if (g.outgoing[src].second == 0) { // No outgoing yet
            g.outgoing[src].first = g.edges.size() - 1;
        }
        g.outgoing[src].second++;

        probabilities[dst] += prob;
    };

    vector<uint64_t> traversalStack {0};
    traversalStack.reserve(64);

    probabilities[0] = 1.0;
    while (traversalStack.size() > 0) {
        uint64_t curIdx = traversalStack.back();
        traversalStack.pop_back();
        g.depthFirst.push_back(curIdx);

        LinearBVHNode *node = &nodes[curIdx];
        float curProb = probabilities[curIdx];
        CHECK_GT(curProb, 0.0);
        CHECK_LE(curProb, 1.0001); // FP error (should be 1.0)

        uint64_t nextHit = 0, nextMiss = 0;
        if (traversalStack.size() > 0) {
            nextMiss = traversalStack.back();
        }

        if (node->nPrimitives == 0) {
            if (dirIsNeg[node->axis]) {
                traversalStack.push_back(curIdx + 1);
                traversalStack.push_back(node->secondChildOffset);
            } else {
                traversalStack.push_back(node->secondChildOffset);
                traversalStack.push_back(curIdx + 1);
            }

            nextHit = traversalStack.back();
            LinearBVHNode *nextHitNode = &nodes[nextHit];

            if (nextMiss == 0) {
                // Guaranteed move down in the BVH
                CHECK_GT(curProb, 0.99); // FP error (should be 1.0)
                addEdge(curIdx, nextHit, curProb);
            } else {
                LinearBVHNode *nextMissNode = &nodes[nextMiss];

                float curSA = node->bounds.SurfaceArea();
                float nextSA = nextHitNode->bounds.SurfaceArea();

                float condHitProb = nextSA / curSA;
                CHECK_LE(condHitProb, 1.0);
                float condMissProb = 1.0 - condHitProb;

                float hitPathProb = curProb * condHitProb;
                float missPathProb = curProb * condMissProb;

                addEdge(curIdx, nextHit, hitPathProb);
                addEdge(curIdx, nextMiss, missPathProb);
            }
        } else if (nextMiss != 0) {
            // Leaf node, guaranteed move up in the BVH
            // Add instance support here
            addEdge(curIdx, nextMiss, curProb);
        } else {
            // Termination point for all traversal paths
            CHECK_EQ(traversalStack.size(), 0);
            CHECK_GT(curProb, 0.99);
        }
        probabilities[curIdx] = -10000;
    }

    for (auto prob : probabilities) {
        CHECK_EQ(prob, -10000);
    }

    return g;
}

TreeletDumpBVH::IntermediateTraversalGraph
TreeletDumpBVH::CreateTraversalGraphCheckSend(const Vector3f &rayDir, int depthReduction) const {
    (void)depthReduction;
    IntermediateTraversalGraph g;
    g.depthFirst.reserve(nodeCount);
    g.outgoing.resize(nodeCount);

    vector<float> probabilities(nodeCount);

    bool dirIsNeg[3] = { rayDir.x < 0, rayDir.y < 0, rayDir.z < 0 };

    auto addEdge = [this, &probabilities, &g](auto src, auto dst, auto prob) {
        g.edges.emplace_back(src, dst, prob);

        if (g.outgoing[src].second == 0) { // No outgoing yet
            g.outgoing[src].first = g.edges.size() - 1;
        }
        g.outgoing[src].second++;

        probabilities[dst] += prob;
    };

    vector<uint64_t> traversalStack {0};
    traversalStack.reserve(64);

    probabilities[0] = 1.0;
    while (traversalStack.size() > 0) {
        uint64_t curIdx = traversalStack.back();
        traversalStack.pop_back();
        g.depthFirst.push_back(curIdx);

        LinearBVHNode *node = &nodes[curIdx];
        float curProb = probabilities[curIdx];
        CHECK_GE(curProb, 0.0);
        CHECK_LE(curProb, 1.0001); // FP error (should be 1.0)

        if (node->nPrimitives == 0) {
            if (dirIsNeg[node->axis]) {
                traversalStack.push_back(curIdx + 1);
                traversalStack.push_back(node->secondChildOffset);
            } else {
                traversalStack.push_back(node->secondChildOffset);
                traversalStack.push_back(curIdx + 1);
            }
        }

        float runningProb = 1.0;
        for (int i = traversalStack.size() - 1; i >= 0; i--) {
            uint64_t nextNode = traversalStack[i];
            LinearBVHNode *nextHitNode = &nodes[nextNode];
            LinearBVHNode *parentHitNode = &nodes[nodeParents[nextNode]];
            
            // FIXME ask Pat about this
            float nextSA = nextHitNode->bounds.SurfaceArea();
            float parentSA = parentHitNode->bounds.SurfaceArea();

            float condHitProb = nextSA / parentSA;
            CHECK_LE(condHitProb, 1.0);
            float pathProb = curProb * runningProb * condHitProb;

            addEdge(curIdx, nextNode, pathProb);
            // runningProb can become 0 here if condHitProb == 1
            // could break, but then edges don't get added and Intersect
            // may crash if it turns out that edge gets taken
            runningProb *= 1.0 - condHitProb;
        }
        CHECK_LE(runningProb, 1.0);
        CHECK_GE(runningProb, 0.0);

        probabilities[curIdx] = -10000;
    }

    for (auto prob : probabilities) {
        CHECK_EQ(prob, -10000);
    }

    return g;
}

TreeletDumpBVH::TraversalGraph
TreeletDumpBVH::CreateTraversalGraph(const Vector3f &rayDir, int depthReduction) const {
    cout << "Starting graph gen\n";
    IntermediateTraversalGraph intermediate;

    //FIXME fix probabilities here on up edges

    switch (traversalAlgo) {
        case TraversalAlgorithm::SendCheck:
            intermediate = CreateTraversalGraphSendCheck(rayDir, depthReduction);
            break;
        case TraversalAlgorithm::CheckSend:
            intermediate = CreateTraversalGraphCheckSend(rayDir, depthReduction);
            break;
    }
    cout << "Intermediate finished\n";

    // Remake graph with contiguous vectors
    TraversalGraph graph;
    auto edgeIter = intermediate.edges.begin();
    while (edgeIter != intermediate.edges.end()) {
        graph.edges.push_back(*edgeIter);
        edgeIter++;
        intermediate.edges.pop_front();
    }

    graph.depthFirst = move(intermediate.depthFirst);

    auto adjacencyIter = intermediate.outgoing.begin();
    while (adjacencyIter != intermediate.outgoing.end()) {
        uint64_t idx = adjacencyIter->first;
        uint64_t weight = adjacencyIter->second;
        graph.outgoing.emplace_back(&graph.edges[idx], weight);
        adjacencyIter++;
        intermediate.outgoing.pop_front();
    }

    printf("Graph gen complete: %lu verts %lu edges\n",
           graph.depthFirst.size(), graph.edges.size());

    return graph;
}

vector<uint32_t>
TreeletDumpBVH::ComputeTreeletsAgglomerative(const TraversalGraph &graph,
                                             uint64_t maxTreeletBytes) const {
    return vector<uint32_t>(nodeCount);
#if 0
    vector<uint64_t> treeletSizes(nodeCount);
    vector<list<int>> treeletNodes(nodeCount);
    vector<unordered_map<uint64_t, float>> adjacencyList(nodeCount);
    vector<unordered_map<uint64_t, decltype(adjacencyList)::value_type::iterator>>
        reverseAdjacencyList(nodeCount);

    list<uint64_t> liveTreelets;
    // Map from original treelet ids to position in liveTreelets list
    vector<decltype(liveTreelets)::iterator> treeletLocs(nodeCount);

    auto addEdge =
        [&adjacencyList, &reverseAdjacencyList](uint64_t src, const Edge *edge) {
        if (edge) {
            auto res = adjacencyList[src].emplace(edge->dst,
                                                  edge->modelWeight);

            reverseAdjacencyList[edge->dst].emplace(src, res.first);
        }
    };

    // Start each node in unique treelet
    for (uint64_t vert : graph.topologicalVertices) {
        liveTreelets.push_back(vert);
        treeletLocs[vert] = --liveTreelets.end();

        treeletSizes[vert] = GetNodeSize(vert);
        treeletNodes[vert].push_back(vert);

        const OutEdges &outEdges = graph.adjacencyList[vert];
        addEdge(vert, outEdges.hitEdge);
        addEdge(vert, outEdges.missEdge);
    }

    while (true) {
        bool treeletsCombined = false;

        auto treeletIter = liveTreelets.begin();
        while (treeletIter != liveTreelets.end()) {
            uint64_t curTreelet = *treeletIter;
            uint64_t srcSize = treeletSizes[curTreelet];

            auto bestEdgeIter = adjacencyList[curTreelet].end();
            float maxWeight = 0;
            auto edgeIter = adjacencyList[curTreelet].begin();
            while (edgeIter != adjacencyList[curTreelet].end()) {
                auto nextIter = next(edgeIter);
                uint64_t dstTreelet = edgeIter->first;
                float dstWeight = edgeIter->second;
                uint64_t dstSize = treeletSizes[dstTreelet];
S
A                if (srcSize + dstSize > maxTreeletBytes) {
                    adjacencyList[curTreelet].erase(edgeIter);
                    size_t erased = reverseAdjacencyList[dstTreelet].erase(curTreelet);
                    CHECK_EQ(erased, 1);
                } else if (dstWeight > maxWeight) {
                    bestEdgeIter = edgeIter;
                    maxWeight = dstWeight;
                }

                edgeIter = nextIter;
            }

            if (bestEdgeIter != adjacencyList[curTreelet].end()) {
                treeletsCombined = true;
                uint64_t mergeTreelet = bestEdgeIter->first;
                CHECK_NE(mergeTreelet, curTreelet);

                liveTreelets.erase(treeletLocs[mergeTreelet]);

                treeletSizes[curTreelet] += treeletSizes[mergeTreelet];

                // Merge in outgoing edges from mergeTreelet
                for (const auto &edge_pair : adjacencyList[mergeTreelet]) {
                    CHECK_NE(edge_pair.first, mergeTreelet); // Cycle

                    size_t erased = reverseAdjacencyList[edge_pair.first].erase(mergeTreelet);
                    CHECK_EQ(erased, 1);

                    if (edge_pair.first == curTreelet) continue;

                    auto res = adjacencyList[curTreelet].emplace(edge_pair.first, edge_pair.second);

                    if (!res.second) {
                        res.first->second += edge_pair.second;
                    } else {
                        reverseAdjacencyList[edge_pair.first].emplace(curTreelet, res.first);
                    }
                }
                treeletNodes[curTreelet].splice(treeletNodes[curTreelet].end(),
                                            move(treeletNodes[mergeTreelet]));

                bool found = false;
                // Update all links pointing to the treelet that was merged
                for (const auto &backLink : reverseAdjacencyList[mergeTreelet]) {
                    const auto &edge = *backLink.second;
                    CHECK_EQ(edge.first, mergeTreelet);
                    float weight = edge.second;

                    adjacencyList[backLink.first].erase(backLink.second);

                    if (backLink.first == curTreelet) {
                        CHECK_EQ(found, false);
                        found = true;
                        continue;
                    }

                    auto res = adjacencyList[backLink.first].emplace(curTreelet, weight);
                    if (!res.second) {
                        res.first->second += weight;
                    } else {
                        reverseAdjacencyList[curTreelet].emplace(backLink.first, res.first);
                    }
                }
                CHECK_EQ(found, true);
            }

            treeletIter = next(treeletIter);
        }

        if (!treeletsCombined) {
            break;
        }
    }

    uint32_t curTreeletID = 1;
    vector<uint32_t> assignment(nodeCount);

    uint64_t totalNodes = 0;

    for (const auto &curNodes : treeletNodes) {
        if (curNodes.empty()) continue;
        for (int node : curNodes) {
            assignment[node] = curTreeletID;
            totalNodes++;
        }

        curTreeletID++;
    }
    CHECK_EQ(totalNodes, nodeCount); // All treelets assigned?
    printf("Generated %u treelets\n", curTreeletID - 1);

    return assignment;
#endif
}

vector<uint32_t>
TreeletDumpBVH::ComputeTreeletsTopological(const TraversalGraph &graph,
                                           uint64_t maxTreeletBytes) const {
    struct OutEdge {
        float weight;
        uint64_t dst;

        OutEdge(const Edge &edge)
            : weight(edge.weight),
              dst(edge.dst)
        {}
    };

    struct EdgeCmp {
        bool operator()(const OutEdge &a, const OutEdge &b) const {
            if (a.weight > b.weight) {
                return true;
            }

            if (a.weight < b.weight) {
                return false;
            }

            return a.dst < b.dst;
        }
    };

    vector<uint32_t> assignment(nodeCount);
    list<uint64_t> depthFirst;
    vector<decltype(depthFirst)::iterator> sortLocs(nodeCount);
    for (uint64_t vert : graph.depthFirst) {
        depthFirst.push_back(vert);
        sortLocs[vert] = --depthFirst.end();
    }

    uint32_t curTreelet = 1;
    while (!depthFirst.empty()) {
        uint64_t curNode = depthFirst.front();
        depthFirst.pop_front();
        assignment[curNode] = curTreelet;

        set<OutEdge, EdgeCmp> cut;
        unordered_map<uint64_t, decltype(cut)::iterator> uniqueLookup;
        InstanceMask includedInstances;

        // Accounts for size of this node + the size of new instances that would be pulled in
        auto getAdditionalSize = [this, &includedInstances](int nodeIdx) {
            const LinearBVHNode &node = nodes[nodeIdx];

            uint64_t totalSize = nodeSizes[nodeIdx];

            for (int primIdx = 0; primIdx < node.nPrimitives; primIdx++) {
                auto &prim  = primitives[node.primitivesOffset + primIdx];
                if (prim->GetType() == PrimitiveType::Transformed) {
                    shared_ptr<TransformedPrimitive> tp = dynamic_pointer_cast<TransformedPrimitive>(prim);
                    shared_ptr<TreeletDumpBVH> instance = dynamic_pointer_cast<TreeletDumpBVH>(tp->GetPrimitive());
                    if (!includedInstances.Get(instance->instanceID)) {
                        totalSize += instance->totalBytes;
                    }
                }
            }

            return totalSize;
        };

        uint64_t remainingBytes = maxTreeletBytes - getAdditionalSize(curNode);
        includedInstances |= nodeInstanceMasks[curNode];

        while (remainingBytes >= sizeof(CloudBVH::TreeletNode)) {
            auto outgoingBounds = graph.outgoing[curNode];
            for (int i = 0; i < outgoingBounds.second; i++) {
                const Edge *edge = outgoingBounds.first + i;

                uint64_t nodeSize = getAdditionalSize(edge->dst);
                if (nodeSize > remainingBytes) break;

                auto preexisting = uniqueLookup.find(edge->dst);
                if (preexisting == uniqueLookup.end()) {
                    auto res = cut.emplace(*edge);
                    CHECK_EQ(res.second, true);
                    uniqueLookup.emplace(edge->dst, res.first);
                } else {
                    auto &iter = preexisting->second;
                    OutEdge update = *iter;
                    CHECK_EQ(update.dst, edge->dst);
                    update.weight += edge->weight;

                    cut.erase(iter);
                    auto res = cut.insert(update);
                    CHECK_EQ(res.second, true);
                    iter = res.first;
                }
            }

            uint64_t usedBytes = 0;
            auto bestEdge = cut.end();

            auto edge = cut.begin();
            while (edge != cut.end()) {
                auto nextEdge = next(edge);
                uint64_t dst = edge->dst;
                uint64_t curBytes = getAdditionalSize(dst);
                float curWeight = edge->weight;

                // This node already belongs to a treelet
                if (assignment[dst] != 0 || curBytes > remainingBytes) {
                    cut.erase(edge);
                    auto eraseRes = uniqueLookup.erase(dst);
                    CHECK_EQ(eraseRes, 1);
                } else {
                    usedBytes = curBytes;
                    bestEdge = edge;
                    break;
                }

                edge = nextEdge;
            }

            // Treelet full
            if (bestEdge == cut.end()) {
                break;
            }

            cut.erase(bestEdge);
            auto eraseRes = uniqueLookup.erase(bestEdge->dst);
            CHECK_EQ(eraseRes, 1);

            curNode = bestEdge->dst;

            depthFirst.erase(sortLocs[curNode]);
            assignment[curNode] = curTreelet;
            remainingBytes -= usedBytes;
            includedInstances |= nodeInstanceMasks[curNode];
        }

        curTreelet++;
    }

    return assignment;
}

vector<uint32_t>
TreeletDumpBVH::ComputeTreeletsTopologicalHierarchical(
        const TraversalGraph &graph, uint64_t maxTreeletBytes) const {
    vector<uint32_t> assignment(nodeCount);
    vector<uint32_t> outgoingWeight(nodeCount);

    struct OutEdge {
        float weight;
        uint64_t dst;

        OutEdge(const Edge &edge)
            : weight(edge.weight),
              dst(edge.dst)
        {}
    };

    struct EdgeCmp {
        bool operator()(const OutEdge &a, const OutEdge &b) const {
            if (a.weight > b.weight) {
                return true;
            }

            if (a.weight < b.weight) {
                return false;
            }

            return a.dst < b.dst;
        }
    };

    for (auto curNodeIter = graph.depthFirst.rbegin(); curNodeIter != graph.depthFirst.rend(); curNodeIter++) {
        uint64_t nodeIdx = *curNodeIter;
    }


    return assignment;
}

vector<uint32_t>
TreeletDumpBVH::ComputeTreeletsGreedySize(
        const TraversalGraph &graph, uint64_t maxTreeletBytes) const {
    return vector<uint32_t>(nodeCount);

#if 0
    static const float ROOT_SIZE = subtreeSizes[0];

    struct OutEdge {
        float weight;
        uint64_t dst;
        uint64_t subtreeSize;

        OutEdge(const Edge &edge, uint64_t subtreeSize)
            : weight(edge.weight),
              dst(edge.dst),
              subtreeSize(subtreeSize)
        {}
    };

    struct EdgeCmp {
        bool operator()(const OutEdge &a, const OutEdge &b) const {
            float aEff = 15.f * a.weight - (float)a.subtreeSize / ROOT_SIZE;
            float bEff = 15.f * b.weight - (float)b.subtreeSize / ROOT_SIZE;
            if (aEff > bEff) {
                return true;
            }

            if (aEff < bEff) {
                return false;
            }

            return a.dst < b.dst;
        }
    };

    vector<uint32_t> assignment(nodeCount);
    list<uint64_t> depthFirst;
    vector<decltype(depthFirst)::iterator> sortLocs(nodeCount);
    for (uint64_t vert : graph.depthFirst) {
        depthFirst.push_back(vert);
        sortLocs[vert] = --depthFirst.end();
    }

    uint32_t curTreelet = 1;
    while (!depthFirst.empty()) {
        uint64_t curNode = depthFirst.front();
        depthFirst.pop_front();
        assignment[curNode] = curTreelet;

        uint64_t remainingBytes = maxTreeletBytes - nodeSizes[curNode];
        set<OutEdge, EdgeCmp> cut;
        unordered_map<uint64_t, decltype(cut)::iterator> uniqueLookup;
        while (remainingBytes >= sizeof(CloudBVH::TreeletNode)) {
            // Add new edges leaving curNode
            auto outgoingBounds = graph.outgoing[curNode];
            for (int i = 0; i < outgoingBounds.second; i++) {
                const Edge *edge = outgoingBounds.first + i;
                if (nodeSizes[edge->dst] > remainingBytes) break;
                auto preexisting = uniqueLookup.find(edge->dst);
                if (preexisting == uniqueLookup.end()) {
                    auto res = cut.emplace(*edge, subtreeSizes[edge->dst]);
                    CHECK_EQ(res.second, true);
                    uniqueLookup.emplace(edge->dst, res.first);
                } else {
                    auto &iter = preexisting->second;
                    OutEdge update = *iter;
                    CHECK_EQ(update.dst, edge->dst);
                    update.weight += edge->weight;

                    cut.erase(iter);
                    auto res = cut.insert(update);
                    CHECK_EQ(res.second, true);
                    iter = res.first;
                }
            }

            uint64_t usedBytes = 0;
            auto bestEdge = cut.end();

            auto edge = cut.begin();
            while (edge != cut.end()) {
                auto nextEdge = next(edge);
                uint64_t dst = edge->dst;
                uint64_t curBytes = nodeSizes[dst];
                float curWeight = edge->weight;

                // This node already belongs to a treelet
                if (assignment[dst] != 0 || curBytes > remainingBytes) {
                    cut.erase(edge);
                    auto eraseRes = uniqueLookup.erase(dst);
                    CHECK_EQ(eraseRes, 1);
                } else {
                    usedBytes = curBytes;
                    bestEdge = edge;
                    break;
                }

                edge = nextEdge;
            }
            // Treelet full
            if (bestEdge == cut.end()) {
                break;
            }

            cut.erase(bestEdge);
            auto eraseRes = uniqueLookup.erase(bestEdge->dst);
            CHECK_EQ(eraseRes, 1);

            curNode = bestEdge->dst;

            depthFirst.erase(sortLocs[curNode]);
            assignment[curNode] = curTreelet;
            remainingBytes -= usedBytes;
        }

        curTreelet++;
    }

    return assignment;
#endif
}
                                                       
vector<uint32_t>
TreeletDumpBVH::ComputeTreelets(const TraversalGraph &graph,
                                uint64_t maxTreeletBytes) const {
    vector<uint32_t> assignment;
    switch (partitionAlgo) {
        case PartitionAlgorithm::OneByOne:
            assignment = ComputeTreeletsTopological(graph, maxTreeletBytes);
            break;
        case PartitionAlgorithm::TopologicalHierarchical:
            assignment = ComputeTreeletsTopologicalHierarchical(graph, maxTreeletBytes);
            break;
        case PartitionAlgorithm::GreedySize:
            assignment = ComputeTreeletsGreedySize(graph, maxTreeletBytes);
            break;
        case PartitionAlgorithm::PseudoAgglomerative:
            assignment = ComputeTreeletsAgglomerative(graph, maxTreeletBytes);
            break;
    }

    uint64_t totalBytes = 0;
    map<uint32_t, uint64_t> sizes;
    unordered_map<uint32_t, InstanceMask> instanceTracker;
    for (int nodeIdx = 0; nodeIdx < nodeCount; nodeIdx++) {
        uint32_t treelet = assignment[nodeIdx];
        CHECK_NE(treelet, 0);

        instanceTracker[treelet] |= nodeInstanceMasks[nodeIdx];

        uint64_t bytes = nodeSizes[nodeIdx];

        sizes[treelet] += bytes;
        totalBytes += bytes;
    }

    for (auto &kv : instanceTracker) {
        uint32_t treelet = kv.first;
        InstanceMask &mask = kv.second;
        for (int instanceIdx = 0; instanceIdx < numInstances; instanceIdx++) {
            if (mask.Get(instanceIdx)) {
                sizes[treelet] += instanceSizes[instanceIdx];
                totalBytes += instanceSizes[instanceIdx];
            }
        }
    }

    printf("Generated %lu treelets: %lu total bytes from %d nodes\n",
           sizes.size(), totalBytes, nodeCount);

    for (auto &sz : sizes) {
        printf("Treelet %u: %lu bytes\n", sz.first, sz.second);
    }

    return assignment;
}

vector<uint32_t> TreeletDumpBVH::OrigAssignTreelets(const uint64_t maxTreeletBytes) const {
    return vector<uint32_t>(nodeCount);

#if 0
    vector<uint32_t> labels(nodeCount);

    /* pass one */
    std::unique_ptr<float []> best_costs(new float[nodeCount]);

    float max_nodes = (float)maxTreeletBytes / sizeof(CloudBVH::TreeletNode);
    const float AREA_EPSILON = nodes[0].bounds.SurfaceArea() * max_nodes / (nodeCount * 10);


    for (int root_index = nodeCount - 1; root_index >= 0; root_index--) {
        const LinearBVHNode & root_node = nodes[root_index];

        std::set<int> cut;
        cut.insert(root_index);
        uint64_t remaining_size = maxTreeletBytes;
        best_costs[root_index] = std::numeric_limits<float>::max();
        InstanceMask includedInstances;
        vector<uint64_t> curSubtreeSizes(subtreeSizes);

        auto getAdditionalSize = [this, &includedInstances](int nodeIdx) {
            const LinearBVHNode &node = nodes[nodeIdx];

            uint64_t totalSize = nodeSizes[nodeIdx];

            for (int primIdx = 0; primIdx < node.nPrimitives; primIdx++) {
                auto &prim  = primitives[node.primitivesOffset + primIdx];
                if (prim->GetType() == PrimitiveType::Transformed) {
                    shared_ptr<TransformedPrimitive> tp = dynamic_pointer_cast<TransformedPrimitive>(prim);
                    shared_ptr<TreeletDumpBVH> instance = dynamic_pointer_cast<TreeletDumpBVH>(tp->GetPrimitive());
                    if (!includedInstances.Get(instance->instanceID)) {
                        totalSize += instance->totalBytes;
                    }
                }
            }

            return totalSize;
        };

        while (true) {
            int best_node_index = -1;
            float best_score = std::numeric_limits<float>::lowest();

            if (remaining_size > 0) {
                for (const auto n : cut) {
                    const float gain = nodes[n].bounds.SurfaceArea() + AREA_EPSILON;
                    const uint64_t price = std::min(curSubtreeSizes[n], remaining_size);
                    const float score = gain / price;
                    if (curNodeSizes[n] <= remaining_size && score > best_score) {
                        best_score = score;
                        best_node_index = n;
                    }
                }
            }

            if (best_node_index == -1) {
                break;
            }

            const LinearBVHNode & best_node = nodes[best_node_index];
            // Need to cache this before UpdateSizes potentially reduces it
            uint64_t bestNodeSize = curNodeSizes[best_node_index];
            cut.erase(best_node_index);
            if (best_node.nPrimitives) {
                UpdateSizes(includedInstances, best_node_index, root_index);
            } else {
                cut.insert(best_node_index + 1);
                cut.insert(best_node.secondChildOffset);
            }

            float this_cost = root_node.bounds.SurfaceArea() + AREA_EPSILON;
            for (const auto n : cut) {
                this_cost += best_costs[n];
            }
            best_costs[root_index] = std::min(best_costs[root_index], this_cost);

            remaining_size -= bestNodeSize;
        }

        UndoUpdateSizes(includedInstances, root_index);
    }

    auto float_equals = [](const float a, const float b) {
        return fabs(a - b) < 1e-4;
    };


    uint32_t current_treelet = 0;

    std::stack<int> q;
    q.push(0);

    int node_count = 0;

    while (not q.empty()) {
        const int root_index = q.top();

        q.pop();

        current_treelet++;

        const LinearBVHNode & root_node = nodes[root_index];
        std::set<int> cut;
        cut.insert(root_index);

        uint64_t remaining_size = maxTreeletBytes;
        const float best_cost = best_costs[root_index];

        float cost = 0;
        std::unordered_set<TreeletDumpBVH *> includedInstances;
        while (true) {
            int best_node_index = -1;
            float best_score = std::numeric_limits<float>::lowest();

            if (remaining_size > 0) {
                for (const auto n : cut) {
                    const float gain = nodes[n].bounds.SurfaceArea() + AREA_EPSILON;
                    const uint64_t price = std::min(curSubtreeSizes[n], remaining_size);
                    const float score = gain / price;
                    if (curNodeSizes[n] <= remaining_size && score > best_score) {
                        best_score = score;
                        best_node_index = n;
                    }
                }
            }

            if (best_node_index == -1) {
                break;
            }

            const LinearBVHNode & best_node = nodes[best_node_index];
            uint64_t bestNodeSize = curNodeSizes[best_node_index];

            cut.erase(best_node_index);
            if (best_node.nPrimitives) {
                UpdateSizes(includedInstances, best_node_index, root_index);
            } else {
                cut.insert(best_node_index + 1);
                cut.insert(best_node.secondChildOffset);
            }

            labels[best_node_index] = current_treelet;

            float this_cost = root_node.bounds.SurfaceArea() + AREA_EPSILON;
            for (const auto n : cut) {
                this_cost += best_costs[n];
            }

            if (float_equals(this_cost, best_cost)) {
                break;
            }

            remaining_size -= bestNodeSize;
        }

        for (const auto n : cut) {
            q.push(n);
        }

        UndoUpdateSizes(includedInstances, root_index);
    }

    /* make sure all of the nodes have a treelet */
    /* for (int i = 0; i < nodeCount; i++) {
        if (not labels[i]) {
            throw std::runtime_error("unassigned node");
        }
    } */

    map<uint32_t, uint64_t> sizes;
    uint64_t totalBytes = 0;
    vector<unordered_set<TreeletDumpBVH *>> instanceTracker(current_treelet);
    for (int nodeIdx = 0; nodeIdx < nodeCount; nodeIdx++) {
        uint32_t treelet = labels[nodeIdx];
        CHECK_NE(treelet, 0);
        const LinearBVHNode &node = nodes[nodeIdx];
        uint64_t bytes = nodeNoInstanceSizes[nodeIdx];
        for (int primIdx = 0; primIdx < node.nPrimitives; primIdx++) {
            auto &prim  = primitives[node.primitivesOffset + primIdx];
            if (prim->GetType() == PrimitiveType::Transformed) {
                shared_ptr<TransformedPrimitive> tp = dynamic_pointer_cast<TransformedPrimitive>(prim);
                shared_ptr<TreeletDumpBVH> instance = dynamic_pointer_cast<TreeletDumpBVH>(tp->GetPrimitive());

                auto res = instanceTracker[treelet - 1].insert(instance.get());
                if (res.second) {
                    bytes += instance->totalBytes;
                }
            }
        }
        
        sizes[treelet] += bytes;
        totalBytes += bytes;
    }

    printf("Original method generated %lu treelets: %lu total bytes from %d nodes\n",
           sizes.size(), totalBytes, nodeCount);

    for (auto &sz : sizes) {
        printf("Treelet %u: %lu bytes\n", sz.first, sz.second);
    }

    return labels;
#endif
}

void UpdateRayCount(const TreeletDumpBVH::RayCountMap &rayCounts,
                    uint64_t src, uint64_t dst) {
    //atomic_uint64_t &rayCount =
    //    const_cast<atomic_uint64_t &>(rayCounts[src].find(dst)->second);
    //rayCount++;
}

bool TreeletDumpBVH::IntersectSendCheck(const Ray &ray,
                                        SurfaceInteraction *isect) const {
    if (!nodes) return false;
    ProfilePhase p(Prof::AccelIntersect);
    bool hit = false;
    Vector3f invDir(1 / ray.d.x, 1 / ray.d.y, 1 / ray.d.z);
    int dirIsNeg[3] = {invDir.x < 0, invDir.y < 0, invDir.z < 0};
    // Follow ray through BVH nodes to find primitive intersections
    int toVisitOffset = 0, currentNodeIndex = 0;
    int nodesToVisit[64];

    int dirIdx = computeIdx(invDir);
    const auto &newLabels = treeletAllocations[dirIdx];
    const auto &oldLabels = origTreeletAllocation;
    
    uint32_t prevNewTreelet = newLabels[currentNodeIndex];
    uint32_t prevOldTreelet = oldLabels[currentNodeIndex];

    while (true) {
        const LinearBVHNode *node = &nodes[currentNodeIndex];
        int prevNodeIndex = currentNodeIndex;
        // Check ray against BVH node
        if (node->bounds.IntersectP(ray, invDir, dirIsNeg)) {
            if (node->nPrimitives > 0) {
                // Intersect ray with primitives in leaf BVH node
                for (int i = 0; i < node->nPrimitives; ++i)
                    if (primitives[node->primitivesOffset + i]->Intersect(
                            ray, isect))
                        hit = true;
                if (toVisitOffset == 0) break;
                currentNodeIndex = nodesToVisit[--toVisitOffset];
            } else {
                // Put far BVH node on _nodesToVisit_ stack, advance to near
                // node
                if (dirIsNeg[node->axis]) {
                    nodesToVisit[toVisitOffset++] = currentNodeIndex + 1;
                    currentNodeIndex = node->secondChildOffset;
                } else {
                    nodesToVisit[toVisitOffset++] = node->secondChildOffset;
                    currentNodeIndex = currentNodeIndex + 1;
                }
            }
        } else {
            if (toVisitOffset == 0) break;
            currentNodeIndex = nodesToVisit[--toVisitOffset];
        }

        UpdateRayCount(rayCounts[dirIdx], prevNodeIndex, currentNodeIndex);

        uint32_t curNewTreelet = newLabels[currentNodeIndex];
        uint32_t curOldTreelet = oldLabels[currentNodeIndex];

        if (curNewTreelet != prevNewTreelet) {
            totalNewRayTransfers++;
        }

        if (curOldTreelet != prevOldTreelet) {
            totalOldRayTransfers++;
        }

        prevNewTreelet = curNewTreelet;
        prevOldTreelet = curOldTreelet;
    }

    return hit;
}

bool TreeletDumpBVH::IntersectPSendCheck(const Ray &ray) const {
    if (!nodes) return false;
    ProfilePhase p(Prof::AccelIntersectP);
    Vector3f invDir(1.f / ray.d.x, 1.f / ray.d.y, 1.f / ray.d.z);
    int dirIsNeg[3] = {invDir.x < 0, invDir.y < 0, invDir.z < 0};
    int nodesToVisit[64];
    int toVisitOffset = 0, currentNodeIndex = 0;

    int dirIdx = computeIdx(invDir);
    const auto &newLabels = treeletAllocations[dirIdx];
    const auto &oldLabels = origTreeletAllocation;

    uint32_t prevNewTreelet = newLabels[currentNodeIndex];
    uint32_t prevOldTreelet = oldLabels[currentNodeIndex];

    while (true) {
        const LinearBVHNode *node = &nodes[currentNodeIndex];
        int prevNodeIndex = currentNodeIndex;

        if (node->bounds.IntersectP(ray, invDir, dirIsNeg)) {
            // Process BVH node _node_ for traversal
            if (node->nPrimitives > 0) {
                for (int i = 0; i < node->nPrimitives; ++i) {
                    if (primitives[node->primitivesOffset + i]->IntersectP(
                            ray)) {
                        return true;
                    }
                }
                if (toVisitOffset == 0) break;
                currentNodeIndex = nodesToVisit[--toVisitOffset];
            } else {
                int prevNodeIndex = currentNodeIndex;
                if (dirIsNeg[node->axis]) {
                    /// second child first
                    nodesToVisit[toVisitOffset++] = currentNodeIndex + 1;
                    currentNodeIndex = node->secondChildOffset;
                } else {
                    nodesToVisit[toVisitOffset++] = node->secondChildOffset;
                    currentNodeIndex = currentNodeIndex + 1;
                }
            }
        } else {
            if (toVisitOffset == 0) break;
            currentNodeIndex = nodesToVisit[--toVisitOffset];
        }

        UpdateRayCount(rayCounts[dirIdx], prevNodeIndex, currentNodeIndex);

        uint32_t curNewTreelet = newLabels[currentNodeIndex];
        uint32_t curOldTreelet = oldLabels[currentNodeIndex];

        if (curNewTreelet != prevNewTreelet) {
            totalNewRayTransfers++;
        }

        if (curOldTreelet != prevOldTreelet) {
            totalOldRayTransfers++;
        }

        prevNewTreelet = curNewTreelet;
        prevOldTreelet = curOldTreelet;
    }

    return false;
}

bool TreeletDumpBVH::IntersectCheckSend(const Ray &ray,
                                        SurfaceInteraction *isect) const {
    if (!nodes) return false;
    ProfilePhase p(Prof::AccelIntersect);
    bool hit = false;
    Vector3f invDir(1 / ray.d.x, 1 / ray.d.y, 1 / ray.d.z);
    int dirIsNeg[3] = {invDir.x < 0, invDir.y < 0, invDir.z < 0};
    // Follow ray through BVH nodes to find primitive intersections
    int toVisitOffset = 0, currentNodeIndex = 0;
    int nodesToVisit[64];

    int dirIdx = computeIdx(invDir);
    const auto &newLabels = treeletAllocations[dirIdx];
    const auto &oldLabels = origTreeletAllocation;
    
    uint32_t prevNewTreelet = newLabels[currentNodeIndex];
    uint32_t prevOldTreelet = oldLabels[currentNodeIndex];

    // Missed the root node
    if (!nodes[currentNodeIndex].bounds.IntersectP(ray, invDir, dirIsNeg)) return false;

    while (true) {
        const LinearBVHNode *node = &nodes[currentNodeIndex];
        int prevNodeIndex = currentNodeIndex;
        // Check ray against BVH node
        if (node->nPrimitives > 0) {
            // Intersect ray with primitives in leaf BVH node
            for (int i = 0; i < node->nPrimitives; ++i)
                if (primitives[node->primitivesOffset + i]->Intersect(
                        ray, isect))
                    hit = true;
        } else {
            // Put far BVH node on _nodesToVisit_ stack, advance to near
            // node
            if (dirIsNeg[node->axis]) {
                nodesToVisit[toVisitOffset++] = currentNodeIndex + 1;
                nodesToVisit[toVisitOffset++] = node->secondChildOffset;
            } else {
                nodesToVisit[toVisitOffset++] = node->secondChildOffset;
                nodesToVisit[toVisitOffset++] = currentNodeIndex + 1;
            }
        }

        while (toVisitOffset > 0) {
            int nodeIndex = nodesToVisit[--toVisitOffset];
            if (nodes[nodeIndex].bounds.IntersectP(ray, invDir, dirIsNeg)) {
                currentNodeIndex = nodeIndex;
                break;
            }
        }

        if (currentNodeIndex == prevNodeIndex) break;

        UpdateRayCount(rayCounts[dirIdx], prevNodeIndex, currentNodeIndex);

        uint32_t curNewTreelet = newLabels[currentNodeIndex];
        uint32_t curOldTreelet = oldLabels[currentNodeIndex];

        if (curNewTreelet != prevNewTreelet) {
            totalNewRayTransfers++;
        }

        if (curOldTreelet != prevOldTreelet) {
            totalOldRayTransfers++;
        }

        prevNewTreelet = curNewTreelet;
        prevOldTreelet = curOldTreelet;
    }

    return hit;
}

bool TreeletDumpBVH::IntersectPCheckSend(const Ray &ray) const {
    if (!nodes) return false;
    ProfilePhase p(Prof::AccelIntersectP);
    Vector3f invDir(1.f / ray.d.x, 1.f / ray.d.y, 1.f / ray.d.z);
    int dirIsNeg[3] = {invDir.x < 0, invDir.y < 0, invDir.z < 0};
    int nodesToVisit[64];
    int toVisitOffset = 0, currentNodeIndex = 0;

    int dirIdx = computeIdx(invDir);
    const auto &newLabels = treeletAllocations[dirIdx];
    const auto &oldLabels = origTreeletAllocation;

    uint32_t prevNewTreelet = newLabels[currentNodeIndex];
    uint32_t prevOldTreelet = oldLabels[currentNodeIndex];

    while (true) {
        const LinearBVHNode *node = &nodes[currentNodeIndex];
        int prevNodeIndex = currentNodeIndex;
        // Check ray against BVH node
        if (node->nPrimitives > 0) {
            // Intersect ray with primitives in leaf BVH node
            for (int i = 0; i < node->nPrimitives; ++i)
                if (primitives[node->primitivesOffset + i]->IntersectP(
                        ray))
                    return true;
        } else {
            // Put far BVH node on _nodesToVisit_ stack, advance to near
            // node
            if (dirIsNeg[node->axis]) {
                nodesToVisit[toVisitOffset++] = currentNodeIndex + 1;
                nodesToVisit[toVisitOffset++] = node->secondChildOffset;
            } else {
                nodesToVisit[toVisitOffset++] = node->secondChildOffset;
                nodesToVisit[toVisitOffset++] = currentNodeIndex + 1;
            }
        }

        while (toVisitOffset > 0) {
            int nodeIndex = nodesToVisit[--toVisitOffset];
            if (nodes[nodeIndex].bounds.IntersectP(ray, invDir, dirIsNeg)) {
                currentNodeIndex = nodeIndex;
                break;
            }
        }

        if (currentNodeIndex == prevNodeIndex) break;

        UpdateRayCount(rayCounts[dirIdx], prevNodeIndex, currentNodeIndex);

        uint32_t curNewTreelet = newLabels[currentNodeIndex];
        uint32_t curOldTreelet = oldLabels[currentNodeIndex];

        if (curNewTreelet != prevNewTreelet) {
            totalNewRayTransfers++;
        }

        if (curOldTreelet != prevOldTreelet) {
            totalOldRayTransfers++;
        }

        prevNewTreelet = curNewTreelet;
        prevOldTreelet = curOldTreelet;
    }

    return false;
}

bool TreeletDumpBVH::Intersect(const Ray &ray, SurfaceInteraction *isect) const {
    if (!rootBVH) {
        return BVHAccel::Intersect(ray, isect);
    }

    switch (traversalAlgo) {
        case TraversalAlgorithm::SendCheck:
            return IntersectSendCheck(ray, isect);
        case TraversalAlgorithm::CheckSend:
            return IntersectCheckSend(ray, isect);
        default:
            CHECK_EQ(true, false);
            return false;
    }
}

bool TreeletDumpBVH::IntersectP(const Ray &ray) const {
    if (!rootBVH) {
        return BVHAccel::IntersectP(ray);
    }

    switch (traversalAlgo) {
        case TraversalAlgorithm::SendCheck:
            return IntersectPSendCheck(ray);
        case TraversalAlgorithm::CheckSend:
            return IntersectPCheckSend(ray);
        default:
            CHECK_EQ(true, false);
            return false;
    }
}

void TreeletDumpBVH::DumpTreelets(const vector<TreeletDumpBVH::TreeletInfo> &treelets) const {
    // Assign IDs to each treelet
    for (const TreeletInfo &treelet : treelets) {
        global::manager.getNextId(ObjectType::Treelet, &treelet);
    }

    vector<unordered_map<int, uint32_t>> treeletNodeLocations(treelets.size());
    vector<unordered_map<TreeletDumpBVH *, uint32_t>> treeletInstanceStarts(treelets.size());
    for (int treeletID = 0; treeletID < treelets.size(); treeletID++) {
        const TreeletInfo &treelet = treelets[treeletID];
        uint32_t totalInstanceNodes = 0;
        for (TreeletDumpBVH *inst : treelet.instances) {
            treeletInstanceStarts[treeletID][inst] = totalInstanceNodes;
            totalInstanceNodes += inst->nodeCount;
        }

        int listIdx = 0;
        for (int nodeIdx : treelet.nodes) {
            treeletNodeLocations[treeletID][nodeIdx] = totalInstanceNodes + listIdx;
            listIdx++;
        }
    }

    for (int treeletID = 0; treeletID < treelets.size(); treeletID++) {
        const TreeletInfo &treelet = treelets[treeletID];
        // Find which triangles / meshes are in treelet
        unordered_map<TriangleMesh *, vector<size_t>> trianglesInTreelet ;
        for (int nodeIdx : treelet.nodes) {
            const LinearBVHNode &node = nodes[nodeIdx];
            for (int primIdx = 0; primIdx < node.nPrimitives; primIdx++) {
                auto &prim = primitives[node.primitivesOffset + primIdx];
                if (prim->GetType() == PrimitiveType::Geometric) {
                    shared_ptr<GeometricPrimitive> gp = dynamic_pointer_cast<GeometricPrimitive>(prim);
                    const Shape *shape = gp->GetShape();
                    const Triangle *tri = dynamic_cast<const Triangle *>(shape);
                    CHECK_NOTNULL(tri);
                    TriangleMesh *mesh = tri->mesh.get();

                    int triNum =
                        (tri->v - tri->mesh->vertexIndices.data()) / 3;

                    CHECK_GE(triNum, 0);
                    CHECK_LT(triNum * 3, tri->mesh->vertexIndices.size());
                    trianglesInTreelet[mesh].push_back(triNum);
                }
            }
        }

        // Get meshes for instances
        unordered_set<TriangleMesh *> instanceMeshes;
        for (const TreeletDumpBVH *inst : treelet.instances) {
            for (int nodeIdx = 0; nodeIdx < inst->nodeCount; nodeIdx++) {
                const LinearBVHNode &node = inst->nodes[nodeIdx];
                for (int primIdx = 0; primIdx < node.nPrimitives; primIdx++) {
                    auto &prim = inst->primitives[node.primitivesOffset + primIdx];
                    if (prim->GetType() != PrimitiveType::Geometric) {
                        throw runtime_error("double nested instancing?");
                    }
                    shared_ptr<GeometricPrimitive> gp = dynamic_pointer_cast<GeometricPrimitive>(prim);
                    const Shape *shape = gp->GetShape();
                    const Triangle *tri = dynamic_cast<const Triangle *>(shape);
                    CHECK_NOTNULL(tri);
                    instanceMeshes.insert(tri->mesh.get());
                }
            }
        }

        unsigned sTreeletID = global::manager.getId(&treelet);
        auto writer = global::manager.GetWriter(ObjectType::Treelet, sTreeletID);
        uint32_t numTriMeshes = trianglesInTreelet.size() + instanceMeshes.size();

        writer->write(numTriMeshes);

        // FIXME add material support
        global::manager.recordDependency(
            ObjectKey {ObjectType::Treelet, sTreeletID},
            ObjectKey {ObjectType::Material, 0});

        unordered_map<TriangleMesh *, unordered_map<size_t, size_t>> triNumRemap;
        unordered_map<TriangleMesh *, uint32_t> triMeshIDs;

        // Write out rewritten meshes with only triangles in treelet
        for (auto &kv : trianglesInTreelet) {
            TriangleMesh *mesh = kv.first;
            vector<size_t> &triNums = kv.second;

            size_t numTris = triNums.size();
            unordered_map<int, size_t> vertexRemap;
            size_t newIdx = 0;
            size_t newTriNum = 0;

            for (auto triNum : triNums) {
                for (int i = 0; i < 3; i++) {
                    int idx = mesh->vertexIndices[triNum * 3 + i];
                    if (vertexRemap.count(idx) == 0) {
                        vertexRemap.emplace(idx, newIdx++);
                    }
                }
                triNumRemap[mesh].emplace(triNum, newTriNum++);
            }
            size_t numVerts = newIdx;
            CHECK_EQ(numVerts, vertexRemap.size());

            vector<int> vertIdxs(numTris * 3);
            vector<Point3f> P(numVerts);
            vector<Vector3f> S(numVerts);
            vector<Normal3f> N(numVerts);
            vector<Point2f> uv(numVerts);
            vector<int> faceIdxs(numVerts);

            for (size_t i = 0; i < numTris; i++) {
                size_t triNum = triNums[i];
                for (int j = 0; j < 3; j++) {
                    int origIdx = mesh->vertexIndices[triNum * 3 + j];
                    int newIdx = vertexRemap.at(origIdx);
                    vertIdxs[i * 3 + j] = newIdx;
                }
                if (mesh->faceIndices.size() > 0) {
                    faceIdxs[i] = mesh->faceIndices[triNum];
                }
            }

            for (auto &kv : vertexRemap) {
                int origIdx = kv.first;
                int newIdx = kv.second;
                P[newIdx] = mesh->p[origIdx];
                if (mesh->s.get()) {
                    S[newIdx] = mesh->s[origIdx];
                }
                if (mesh->n.get()) {
                    N[newIdx] = mesh->n[origIdx];
                }
                if (mesh->uv.get()) {
                    uv[newIdx] = mesh->uv[origIdx];
                }
            }

            shared_ptr<TriangleMesh> newMesh = make_shared<TriangleMesh>(
                Transform(), numTris, vertIdxs.data(), numVerts,
                P.data(), mesh->s.get() ? S.data() : nullptr,
                mesh->n.get() ? N.data() : nullptr,
                mesh->uv.get() ? uv.data() : nullptr, mesh->alphaMask,
                mesh->shadowAlphaMask,
                mesh->faceIndices.size() > 0 ? faceIdxs.data() : nullptr);


            // Give triangle mesh an ID
            uint32_t sMeshID = global::manager.getNextId(ObjectType::TriangleMesh);

            triMeshIDs[mesh] = sMeshID;

            protobuf::TriangleMesh tmProto = to_protobuf(*newMesh);
            tmProto.set_id(sMeshID);
            tmProto.set_material_id(0);
            writer->write(tmProto);
        }

        // Write out the full triangle meshes for all the instances referenced by this treelet
        for (TriangleMesh *instMesh : instanceMeshes) {
            uint32_t sMeshID = global::manager.getNextId(ObjectType::TriangleMesh);

            triMeshIDs[instMesh] = sMeshID;

            protobuf::TriangleMesh tmProto = to_protobuf(*instMesh);
            tmProto.set_id(sMeshID);
            tmProto.set_material_id(0);
            writer->write(tmProto);
        }

        // Write out nodes for instances
        for (TreeletDumpBVH *inst : treelet.instances) {
            for (int nodeIdx = 0; nodeIdx < inst->nodeCount; nodeIdx++) {
                const LinearBVHNode &instNode = inst->nodes[nodeIdx];

                protobuf::BVHNode nodeProto;
                *nodeProto.mutable_bounds() = to_protobuf(instNode.bounds);
                nodeProto.set_axis(instNode.axis);

                for (int primIdx = 0; primIdx < instNode.nPrimitives; primIdx++) {
                    auto &prim = inst->primitives[instNode.primitivesOffset + primIdx];
                    shared_ptr<GeometricPrimitive> gp =
                        dynamic_pointer_cast<GeometricPrimitive>(prim);
                    CHECK_NOTNULL(gp.get());
                    const Shape *shape = gp->GetShape();
                    const Triangle *tri = dynamic_cast<const Triangle *>(shape);
                    CHECK_NOTNULL(tri);
                    TriangleMesh *mesh = tri->mesh.get();

                    uint32_t sMeshID = triMeshIDs.at(mesh);
                    int triNum = (tri->v - mesh->vertexIndices.data()) / 3;

                    protobuf::Triangle triProto;
                    triProto.set_mesh_id(sMeshID);
                    triProto.set_tri_number(triNum);
                    *nodeProto.add_triangles() = triProto;
                }

                writer->write(nodeProto);
            }
        }

        // Write out nodes for treelet
        for (int nodeIdx : treelet.nodes) {
            const LinearBVHNode &node = nodes[nodeIdx];

            protobuf::BVHNode nodeProto;
            *nodeProto.mutable_bounds() = to_protobuf(node.bounds);
            nodeProto.set_axis(node.axis);

            for (int primIdx = 0; primIdx < node.nPrimitives; primIdx++) {
                auto &prim = primitives[node.primitivesOffset + primIdx];
                if (prim->GetType() == PrimitiveType::Transformed) {
                    shared_ptr<TransformedPrimitive> tp =
                        dynamic_pointer_cast<TransformedPrimitive>(prim);
                    shared_ptr<TreeletDumpBVH> bvh =
                        dynamic_pointer_cast<TreeletDumpBVH>(tp->GetPrimitive());

                    CHECK_NOTNULL(bvh.get());

                    protobuf::TransformedPrimitive tpProto;
                    uint64_t instanceRef = treeletID;
                    instanceRef <<= 32;
                    instanceRef |= treeletInstanceStarts[treeletID].at(bvh.get());
                    tpProto.set_root_ref(instanceRef);
                    *tpProto.mutable_transform() = to_protobuf(tp->GetTransform());

                    *nodeProto.add_transformed_primitives() = tpProto;
                } else {
                    shared_ptr<GeometricPrimitive> gp =
                        dynamic_pointer_cast<GeometricPrimitive>(prim);
                    const Shape *shape = gp->GetShape();
                    const Triangle *tri = dynamic_cast<const Triangle *>(shape);
                    CHECK_NOTNULL(tri);
                    TriangleMesh *mesh = tri->mesh.get();

                    uint32_t sMeshID = triMeshIDs.at(mesh);
                    int origTriNum = (tri->v - mesh->vertexIndices.data()) / 3;
                    int newTriNum = triNumRemap.at(mesh).at(origTriNum);

                    protobuf::Triangle triProto;
                    triProto.set_mesh_id(sMeshID);
                    triProto.set_tri_number(newTriNum);
                    *nodeProto.add_triangles() = triProto;
                }
            }

            if (node.nPrimitives == 0) {
                uint32_t leftTreeletID = treeletAllocations[treelet.dirIdx][nodeIdx + 1];
                if (leftTreeletID != treeletID) {
                    uint32_t sTreeletID = global::manager.getId(&treelets[leftTreeletID]);
                    uint64_t leftRef = sTreeletID;
                    leftRef <<= 32;
                    leftRef |=
                        treeletNodeLocations[leftTreeletID].at(nodeIdx + 1);
                    nodeProto.set_left_ref(leftRef);
                }

                uint32_t rightTreeletID = treeletAllocations[treelet.dirIdx][node.secondChildOffset];
                if (rightTreeletID != treeletID) {
                    uint32_t sTreeletID = global::manager.getId(&treelets[rightTreeletID]);
                    uint64_t rightRef = sTreeletID;
                    rightRef <<= 32;
                    rightRef |=
                        treeletNodeLocations[rightTreeletID].at(node.secondChildOffset);
                    nodeProto.set_right_ref(rightRef);
                }
            }
            writer->write(nodeProto);
        }
    }
}


}
