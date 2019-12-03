#include "cloud/treelettestbvh.h"
#include "paramset.h"
#include "stats.h"
#include <algorithm>
#include <fstream>

using namespace std;

namespace pbrt {

STAT_COUNTER("BVH/Total Ray Transfers (new)", totalNewRayTransfers);
STAT_COUNTER("BVH/Total Ray Transfers (old)", totalOldRayTransfers);

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

TreeletTestBVH::TreeletTestBVH(vector<shared_ptr<Primitive>> &&p,
                               int maxTreeletBytes,
                               TreeletTestBVH::TraversalAlgorithm travAlgo,
                               TreeletTestBVH::PartitionAlgorithm partAlgo,
                               int maxPrimsInNode,
                               SplitMethod splitMethod,
                               bool dumpBVH,
                               const string &dumpBVHPath)
        : BVHAccel(p, maxPrimsInNode, splitMethod),
          traversalAlgo(travAlgo),
          partitionAlgo(partAlgo)
{
    if (dumpBVH) {
        ofstream file(dumpBVHPath);
        file.write((char *)&nodeCount, sizeof(int));
        file.write((char *)nodes, nodeCount * sizeof(LinearBVHNode));
        file.close();
    }
    SetNodeSizes();
    SetSubtreeSizes();
    AllocateTreelets(maxTreeletBytes);
}

TreeletTestBVH::TreeletTestBVH(vector<shared_ptr<Primitive>> &&p,
                               LinearBVHNode *deserializedNodes,
                               int deserializedNodeCount,
                               int maxTreeletBytes,
                               TraversalAlgorithm travAlgo,
                               PartitionAlgorithm partAlgo)
    : BVHAccel(move(p), deserializedNodes, deserializedNodeCount),
      traversalAlgo(travAlgo),
      partitionAlgo(partAlgo)
{
    SetNodeSizes();
    AllocateTreelets(maxTreeletBytes);
}

shared_ptr<TreeletTestBVH> CreateTreeletTestBVH(
    vector<shared_ptr<Primitive>> prims, const ParamSet &ps) {
    int maxTreeletBytes = ps.FindOneInt("maxtreeletbytes", 1'000'000'000);

    string travAlgoName = ps.FindOneString("traversal", "sendcheck");
    TreeletTestBVH::TraversalAlgorithm travAlgo;
    if (travAlgoName == "sendcheck")
        travAlgo = TreeletTestBVH::TraversalAlgorithm::SendCheck;
    else if (travAlgoName == "checksend")
        travAlgo = TreeletTestBVH::TraversalAlgorithm::CheckSend;
    else {
        Warning("BVH traversal algorithm \"%s\" unknown. Using \"SendCheck\".",
                travAlgoName.c_str());
    }

    string partAlgoName = ps.FindOneString("partition", "onebyone");
    TreeletTestBVH::PartitionAlgorithm partAlgo;
    if (partAlgoName == "onebyone")
        partAlgo = TreeletTestBVH::PartitionAlgorithm::OneByOne;
    else if (partAlgoName == "topohierarchical")
        partAlgo = TreeletTestBVH::PartitionAlgorithm::TopologicalHierarchical;
    else if (partAlgoName == "greedysize")
        partAlgo = TreeletTestBVH::PartitionAlgorithm::GreedySize;
    else if (partAlgoName == "agglomerative")
        partAlgo = TreeletTestBVH::PartitionAlgorithm::PseudoAgglomerative;
    else {
        Warning("BVH partition algorithm \"%s\" unknown. Using \"OneByOne\".",
                partAlgoName.c_str());
    }

    string serializedBVHPath = ps.FindOneString("bvhnodes", "");
    if (serializedBVHPath != "") {
        ifstream bvhfile(serializedBVHPath);
        int nodeCount;
        bvhfile.read((char *)&nodeCount, sizeof(int));
        LinearBVHNode *nodes = AllocAligned<LinearBVHNode>(nodeCount);
        bvhfile.read((char *)nodes, nodeCount * sizeof(LinearBVHNode));
        bvhfile.close();

        return make_shared<TreeletTestBVH>(move(prims), nodes, nodeCount,
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
            return make_shared<TreeletTestBVH>(move(prims), maxTreeletBytes,
                                               travAlgo, partAlgo,
                                               maxPrimsInNode, splitMethod,
                                               true, dumpBVHPath);
        } else {
            return make_shared<TreeletTestBVH>(move(prims), maxTreeletBytes,
                                               travAlgo, partAlgo,
                                               maxPrimsInNode, splitMethod);
        }
    }
}

void TreeletTestBVH::SetNodeSizes() {
    nodeSizes.reserve(nodeCount);
    for (int i = 0; i < nodeCount; i++) {
        nodeSizes.push_back(GetNodeSize(i));
    }
}

void TreeletTestBVH::SetSubtreeSizes() {
    subtreeSizes.resize(nodeCount);

    for (int idx = nodeCount - 1; idx >= 0; idx--) {
        const LinearBVHNode &node = nodes[idx];
        subtreeSizes[idx] = nodeSizes[idx];
        if (node.nPrimitives == 0) {
            subtreeSizes[idx] += subtreeSizes[idx + 1] +
                                 subtreeSizes[node.secondChildOffset];
        }
    }
}

void TreeletTestBVH::AllocateTreelets(int maxTreeletBytes) {
    origTreeletAllocation = OrigAssignTreelets(maxTreeletBytes);
    for (unsigned i = 0; i < 8; i++) {
        Vector3f dir = computeRayDir(i);
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

        treeletAllocations[i] = ComputeTreelets(graph, maxTreeletBytes);
    }
}

TreeletTestBVH::IntermediateTraversalGraph
TreeletTestBVH::CreateTraversalGraphSendCheck(const Vector3f &rayDir, int depthReduction) const {
    (void)depthReduction;
    IntermediateTraversalGraph g;
    g.depthFirst.reserve(nodeCount);
    g.predSiblings.resize(nodeCount);
    g.parents.resize(nodeCount);
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
            g.predSiblings[*(traversalStack.end() - 1)] = *(traversalStack.end() - 2);
            g.parents[*(traversalStack.end() - 1)] = curIdx;
            g.parents[*(traversalStack.end() - 2)] = curIdx;

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

TreeletTestBVH::IntermediateTraversalGraph
TreeletTestBVH::CreateTraversalGraphCheckSend(const Vector3f &rayDir, int depthReduction) const {
    (void)depthReduction;
    IntermediateTraversalGraph g;
    g.depthFirst.reserve(nodeCount);
    g.predSiblings.resize(nodeCount);
    g.parents.resize(nodeCount);
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
            g.predSiblings[*(traversalStack.end() - 1)] = *(traversalStack.end() - 2);
            g.parents[*(traversalStack.end() - 1)] = curIdx;
            g.parents[*(traversalStack.end() - 2)] = curIdx;
        }

        float runningProb = 1.0;
        for (int i = traversalStack.size() - 1; i >= 0; i--) {
            uint64_t nextNode = traversalStack[i];
            LinearBVHNode *nextHitNode = &nodes[nextNode];
            LinearBVHNode *parentHitNode = &nodes[g.parents[nextNode]];
            
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

TreeletTestBVH::TraversalGraph
TreeletTestBVH::CreateTraversalGraph(const Vector3f &rayDir, int depthReduction) const {
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
    graph.predSiblings = move(intermediate.predSiblings);
    graph.parents = move(intermediate.parents);

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

uint64_t TreeletTestBVH::GetNodeSize(int nodeIdx) const {
    const LinearBVHNode * node = &nodes[nodeIdx];
    const uint64_t nodeSize = sizeof(CloudBVH::TreeletNode);
    // Assume on average 2 unique vertices, normals etc per triangle
    const uint64_t primSize = 3 * sizeof(int) + 2 * (sizeof(Point3f) +
            sizeof(Normal3f) + sizeof(Vector3f) + sizeof(Point2f));

    return nodeSize + node->nPrimitives * primSize;
}

vector<uint32_t>
TreeletTestBVH::ComputeTreeletsAgglomerative(const TraversalGraph &graph,
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

                if (srcSize + dstSize > maxTreeletBytes) {
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
TreeletTestBVH::ComputeTreeletsTopological(const TraversalGraph &graph,
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

        uint64_t remainingBytes = maxTreeletBytes - nodeSizes[curNode];
        set<OutEdge, EdgeCmp> cut;
        unordered_map<uint64_t, decltype(cut)::iterator> uniqueLookup;
        while (remainingBytes >= sizeof(CloudBVH::TreeletNode)) {
            auto outgoingBounds = graph.outgoing[curNode];
            for (int i = 0; i < outgoingBounds.second; i++) {
                const Edge *edge = outgoingBounds.first + i;
                if (nodeSizes[edge->dst] > remainingBytes) break;
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
}

vector<uint32_t>
TreeletTestBVH::ComputeTreeletsTopologicalHierarchical(
        const TraversalGraph &graph, uint64_t maxTreeletBytes) const {
    vector<uint32_t> assignment;

    return assignment;
}

vector<uint32_t>
TreeletTestBVH::ComputeTreeletsGreedySize(
        const TraversalGraph &graph, uint64_t maxTreeletBytes) const {
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
}
                                                       
vector<uint32_t>
TreeletTestBVH::ComputeTreelets(const TraversalGraph &graph,
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

    map<uint32_t, uint64_t> sizes;
    uint64_t totalBytes = 0;
    for (int node = 0; node < nodeCount; node++) {
        uint32_t treelet = assignment[node];
        uint64_t bytes = nodeSizes[node];
        sizes[treelet] += bytes;
        totalBytes += bytes;
    }

    printf("Generated %lu treelets: %lu total bytes from %d nodes\n",
           sizes.size(), totalBytes, nodeCount);

    for (auto &sz : sizes) {
        printf("Treelet %u: %lu bytes\n", sz.first, sz.second);
    }

    return assignment;
}

vector<uint32_t> TreeletTestBVH::OrigAssignTreelets(const uint64_t maxTreeletBytes) const {
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

        while (true) {
            int best_node_index = -1;
            float best_score = std::numeric_limits<float>::lowest();

            if (remaining_size > 0) {
                for (const auto n : cut) {
                    const float gain = nodes[n].bounds.SurfaceArea() + AREA_EPSILON;
                    const uint64_t price = std::min(subtreeSizes[n], remaining_size);
                    const float score = gain / price;
                    if (score > best_score) {
                        best_score = score;
                        best_node_index = n;
                    }
                }
            }

            if (best_node_index == -1) {
                break;
            }

            const LinearBVHNode & best_node = nodes[best_node_index];
            cut.erase(best_node_index);
            if (not best_node.nPrimitives) {
                cut.insert(best_node_index + 1);
                cut.insert(best_node.secondChildOffset);
            }

            float this_cost = root_node.bounds.SurfaceArea() + AREA_EPSILON;
            for (const auto n : cut) {
                this_cost += best_costs[n];
            }
            best_costs[root_index] = std::min(best_costs[root_index], this_cost);

            if (remaining_size < nodeSizes[best_node_index]) break;
            remaining_size -= nodeSizes[best_node_index];
        }
    }

    auto float_equals = [](const float a, const float b) {
        return fabs(a - b) < 1e-4;
    };


    vector<uint32_t> labels(nodeCount);
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
        while (true) {
            int best_node_index = -1;
            float best_score = std::numeric_limits<float>::lowest();

            if (remaining_size > 0) {
                for (const auto n : cut) {
                    const float gain = nodes[n].bounds.SurfaceArea() + AREA_EPSILON;
                    const uint64_t price = std::min(subtreeSizes[n], remaining_size);
                    const float score = gain / price;
                    if (score > best_score) {
                        best_score = score;
                        best_node_index = n;
                    }
                }
            }

            if (best_node_index == -1) {
                break;
            }

            const LinearBVHNode & best_node = nodes[best_node_index];
            cut.erase(best_node_index);
            if (not best_node.nPrimitives) {
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

            if (remaining_size < nodeSizes[best_node_index]) break;

            remaining_size -= nodeSizes[best_node_index];
        }

        for (const auto n : cut) {
            q.push(n);
        }
    }

    /* make sure all of the nodes have a treelet */
    /* for (int i = 0; i < nodeCount; i++) {
        if (not labels[i]) {
            throw std::runtime_error("unassigned node");
        }
    } */

    map<uint32_t, uint64_t> sizes;
    uint64_t totalBytes = 0;
    for (int node = 0; node < nodeCount; node++) {
        uint32_t treelet = labels[node];
        uint64_t bytes = nodeSizes[node];
        sizes[treelet] += bytes;
        totalBytes += bytes;
    }

    printf("Original method generated %lu treelets: %lu total bytes from %d nodes\n",
           sizes.size(), totalBytes, nodeCount);

    for (auto &sz : sizes) {
        printf("Treelet %u: %lu bytes\n", sz.first, sz.second);
    }

    return labels;
}

void UpdateRayCount(const TreeletTestBVH::RayCountMap &rayCounts,
                    uint64_t src, uint64_t dst) {
    //atomic_uint64_t &rayCount =
    //    const_cast<atomic_uint64_t &>(rayCounts[src].find(dst)->second);
    //rayCount++;
}

bool TreeletTestBVH::IntersectSendCheck(const Ray &ray,
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

bool TreeletTestBVH::IntersectPSendCheck(const Ray &ray) const {
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

bool TreeletTestBVH::IntersectCheckSend(const Ray &ray,
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

bool TreeletTestBVH::IntersectPCheckSend(const Ray &ray) const {
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

bool TreeletTestBVH::Intersect(const Ray &ray, SurfaceInteraction *isect) const {
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

bool TreeletTestBVH::IntersectP(const Ray &ray) const {
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

}
