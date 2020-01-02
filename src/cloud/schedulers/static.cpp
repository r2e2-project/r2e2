#include "cloud/schedulers/static.h"

#include <fstream>
#include <string>

#include "util/exception.h"

using namespace std;
using namespace pbrt;

StaticScheduler::StaticScheduler(const string &path) {
    ifstream fin{path};

    if (!fin.good()) {
        throw runtime_error("static file was not found");
    }

    vector<double> probs;
    size_t treeletCount = 0;

    fin >> treeletCount;

    probs.resize(treeletCount);

    for (size_t i = 0; i < treeletCount; i++) {
        size_t groupSize = 0;
        TreeletId id = 0;
        double prob = 0.f;

        fin >> prob >> groupSize;

        if (groupSize != 1) {
            throw runtime_error(
                "static scheduler doesn't support treelet grouping");
        }

        fin >> id;
        probs[id] = prob;
    }

    map<TreeletId, double> probsMap;

    for (size_t tid = 0; tid < probs.size(); tid++) {
        probsMap.emplace(tid, probs[tid]);
        allocator.addTreelet(tid);
    }

    allocator.setTargetWeights(move(probsMap));
}

Optional<Schedule> StaticScheduler::schedule(const size_t maxWorkers,
                                             const vector<TreeletStats> &) {
    if (scheduledOnce) return {false};
    scheduledOnce = true;

    Schedule result;
    result.resize(maxWorkers, 0);

    for (size_t wid = 0; wid < maxWorkers; wid++) {
        const auto tid = allocator.allocate(wid);
        result[tid]++;
    }

    return result;
}
