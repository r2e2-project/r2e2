#include "uniform.hh"

#include "util/exception.hh"

using namespace std;
using namespace r2t2;

Optional<Schedule> UniformScheduler::schedule(
    const size_t maxWorkers, const vector<TreeletStats> &treelets) {
    if (scheduledOnce) return {false};

    if (maxWorkers < treelets.size()) {
        throw runtime_error("Not enough workers for uniform scheduler");
    }

    const size_t share = maxWorkers / treelets.size();
    vector<size_t> results(treelets.size(), share);

    const size_t leftover = maxWorkers - share * treelets.size();

    for (size_t i = 0; i < leftover; i++) {
        results[i]++;
    }

    scheduledOnce = true;
    return {true, move(results)};
}
