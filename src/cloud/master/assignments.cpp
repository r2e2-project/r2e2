#include "cloud/lambda-master.h"

#include "cloud/allocator.h"

using namespace std;
using namespace pbrt;

void LambdaMaster::loadStaticAssignment(const uint32_t assignmentId,
                                        const uint32_t numWorkers) {
    ifstream fin{global::manager.getScenePath() + "/" +
                 global::manager.getFileName(ObjectType::StaticAssignment,
                                             assignmentId)};

    if (!fin.good()) {
        throw runtime_error("Static assignment file was not found");
    }

    vector<vector<TreeletId>> groups;
    vector<double> probs;
    size_t groupCount = 0;

    fin >> groupCount;

    groups.resize(groupCount);
    probs.resize(groupCount);

    for (size_t i = 0; i < groupCount; i++) {
        size_t groupSize = 0;
        fin >> probs[i] >> groupSize;

        auto &group = groups[i];
        group.resize(groupSize);

        for (size_t j = 0; j < groupSize; j++) {
            fin >> group[j];
        }
    }

    Allocator allocator;

    map<TreeletId, double> probsMap;

    for (size_t gid = 0; gid < probs.size(); gid++) {
        probsMap.emplace(gid, probs[gid]);
        allocator.addTreelet(gid);
    }

    allocator.setTargetWeights(move(probsMap));

    for (size_t wid = 0; wid < numWorkers; wid++) {
        const auto gid = allocator.allocate(wid);

        auto &workerAssignments = staticAssignments[wid];
        for (const auto t : groups[gid]) {
            workerAssignments.push_back(t);
        }
    }

    if (allocator.anyUnassignedTreelets()) {
        throw runtime_error("Unassigned treelets!");
    }

    /* XXX count empty workers */
}
