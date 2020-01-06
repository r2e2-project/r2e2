#include "cloud/schedulers/dynamic.h"

using namespace std;
using namespace pbrt;

DynamicScheduler::DynamicScheduler(const string &path)
    : staticScheduler(path) {}

Optional<Schedule> DynamicScheduler::schedule(
    const size_t maxWorkers, const vector<TreeletStats> &stats) {
    switch (stage) {
    case INITIAL: {
        Schedule result(stats.size(), 0);
        result[0] = maxWorkers;  // all workers for treelet zero
        stage = ROOT_ONLY;
        return {true, move(result)};
    }

    case ROOT_ONLY: {
        if (stats[0].enqueued.rays == stats[0].dequeued.rays) {
            stage = STATIC;
            return staticScheduler.schedule(maxWorkers, stats);
        }

        break;
    }

    case STATIC:
        return staticScheduler.schedule(maxWorkers, stats);
    }

    return {false};
}
