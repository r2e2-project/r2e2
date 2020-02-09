#include "cloud/schedulers/static_multi.h"

#include <fstream>
#include <string>

#include "util/exception.h"

using namespace std;
using namespace pbrt;

StaticMultiScheduler::StaticMultiScheduler(const string &path) {
    ifstream fin{path};

    if (!fin.good()) {
        throw runtime_error("static file was not found");
    }


    uint64_t treeletCount;
    fin >> treeletCount;

    vector<double> probs(treeletCount);

    for (size_t i = 0; i < treeletCount; i++) {
        TreeletId id = 0;
        double prob = 0.f;

        fin >> id >> prob;
        probs[id] = prob;
    }

    map<TreeletId, double> probsMap;

    for (size_t tid = 0; tid < probs.size(); tid++) {
        probsMap.emplace(tid, probs[tid]);
        allocator.addTreelet(tid);
    }

    allocator.setTargetWeights(move(probsMap));
}

Optional<Schedule> StaticMultiScheduler::schedule(
    const size_t maxWorkers, const vector<TreeletStats> &stats) {
    if (scheduledOnce) return {false};
    scheduledOnce = true;

    Schedule result;
    result.resize(stats.size(), 0);

    for (size_t wid = 0; wid < maxWorkers; wid++) {
        const auto tid = allocator.allocate(wid);
        result[tid]++;
    }

    return result;
}
