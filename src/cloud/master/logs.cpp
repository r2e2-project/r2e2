#include "cloud/lambda-master.h"

#include <iomanip>

#include "messages/utils.h"

using namespace std;
using namespace std::chrono;
using namespace pbrt;
using namespace PollerShortNames;

void LambdaMaster::logEnqueue(const WorkerId workerId, const RayBagInfo &info) {
    auto &worker = workers[workerId];
    auto &treelet = treelets[info.treeletId];

    worker.lastStats.first = true;
    treelet.lastStats.first = true;

    if (info.sampleBag) {
        worker.stats.samples.count += info.rayCount;
        worker.stats.samples.bytes += info.bagSize;
        aggregatedStats.samples.count += info.rayCount;
        aggregatedStats.samples.bytes += info.bagSize;

        lastFinishedRay = steady_clock::now();
    } else {
        worker.stats.enqueued.count += info.rayCount;
        worker.stats.enqueued.bytes += info.bagSize;
        treelet.stats.enqueued.count += info.rayCount;
        treelet.stats.enqueued.bytes += info.bagSize;
        aggregatedStats.enqueued.count += info.rayCount;
        aggregatedStats.enqueued.bytes += info.bagSize;
    }
}

void LambdaMaster::logAssign(const WorkerId workerId, const RayBagInfo &info) {
    auto &worker = workers[workerId];

    worker.assignedRayBags.insert(info);

    worker.lastStats.first = true;
    worker.stats.assigned.count += info.rayCount;
    worker.stats.assigned.bytes += info.bagSize;

    aggregatedStats.assigned.bytes += info.rayCount;
    aggregatedStats.assigned.bytes += info.bagSize;
}

void LambdaMaster::logDequeue(const WorkerId workerId, const RayBagInfo &info) {
    auto &worker = workers[workerId];
    auto &treelet = treelets[info.treeletId];

    worker.assignedRayBags.erase(info);

    worker.lastStats.first = true;
    treelet.lastStats.first = true;

    worker.stats.dequeued.count += info.rayCount;
    worker.stats.dequeued.bytes += info.bagSize;
    treelet.stats.dequeued.count += info.rayCount;
    treelet.stats.dequeued.bytes += info.bagSize;
    aggregatedStats.dequeued.count += info.rayCount;
    aggregatedStats.dequeued.bytes += info.bagSize;
}

ResultType LambdaMaster::handleWorkerStats() {
    workerStatsWriteTimer.reset();

    const auto t =
        duration_cast<milliseconds>(steady_clock::now() - startTime).count();

    const float T = static_cast<float>(config.workerStatsWriteInterval);

    for (size_t workerId = 1; workerId <= numberOfWorkers; workerId++) {
        if (!workers[workerId].lastStats.first) {
            continue; /* nothing new to log */
        }

        const WorkerStats stats =
            workers[workerId].stats - workers[workerId].lastStats.second;

        workers[workerId].lastStats.second = workers[workerId].stats;
        workers[workerId].lastStats.first = false;

        /* timestamp,workerId,raysEnqueued,raysAssigned,raysDequeued,
        bytesEnqueued,bytesAssigned,bytesDequeued,numSamples,bytesSamples */
        wsStream << t << ',' << workerId << ',' << fixed
                 << (stats.enqueued.count / T) << ','
                 << (stats.assigned.count / T) << ','
                 << (stats.dequeued.count / T) << ','
                 << (stats.enqueued.bytes / T) << ','
                 << (stats.assigned.bytes / T) << ','
                 << (stats.dequeued.bytes / T) << ','
                 << (stats.samples.count / T) << ','
                 << (stats.samples.bytes / T) << '\n';
    }

    for (size_t treeletId = 0; treeletId < treelets.size(); treeletId++) {
        if (!treelets[treeletId].lastStats.first) {
            continue; /* nothing new to log */
        }

        const TreeletStats stats =
            treelets[treeletId].stats - treelets[treeletId].lastStats.second;

        treelets[treeletId].lastStats.second = treelets[treeletId].stats;
        treelets[treeletId].lastStats.first = false;

        /* timestamp,treeletId,raysEnqueued,raysDequeued,bytesEnqueued,
           bytesDequeued */
        tlStream << t << ',' << treeletId << ',' << fixed
                 << (stats.enqueued.count / T) << ','
                 << (stats.dequeued.count / T) << ','
                 << (stats.enqueued.bytes / T) << ','
                 << (stats.dequeued.bytes / T) << '\n';
    }

    return ResultType::Continue;
}

void LambdaMaster::dumpJobSummary() const {
    protobuf::JobSummary proto;

    proto.set_total_time(
        duration_cast<milliseconds>(lastFinishedRay - startTime).count() /
        1000.0);

    proto.set_launch_time(
        duration_cast<milliseconds>(generationStart - startTime).count() /
        1000.0);

    proto.set_ray_time(
        duration_cast<milliseconds>(lastFinishedRay - generationStart).count() /
        1000.0);

    proto.set_num_lambdas(numberOfWorkers);
    proto.set_total_paths(scene.totalPaths);
    proto.set_finished_paths(aggregatedStats.finishedPaths);
    proto.set_finished_rays(aggregatedStats.samples.count);
    proto.set_num_enqueues(aggregatedStats.enqueued.count);

    ofstream fout{config.jobSummaryPath};
    fout << protoutil::to_json(proto) << endl;
}

void LambdaMaster::printJobSummary() const {
    const static double LAMBDA_UNIT_COST = 0.00004897; /* $/lambda/sec */

    cerr << "* Job summary: " << endl;
    cerr << "  >> Average ray throughput: "
         << (1.0 * 0 / numberOfWorkers /
             duration_cast<seconds>(lastFinishedRay - generationStart).count())
         << " rays/core/s" << endl;

    if (lastFinishedRay >= startTime) {
        cerr << "  >> Total run time: " << fixed << setprecision(2)
             << (duration_cast<milliseconds>(lastFinishedRay - startTime)
                     .count() /
                 1000.0)
             << " seconds" << endl;
    }

    if (generationStart >= startTime) {
        cerr << "      - Launching lambdas: " << fixed << setprecision(2)
             << (duration_cast<milliseconds>(generationStart - startTime)
                     .count() /
                 1000.0)
             << " seconds" << endl;
    }

    if (lastFinishedRay >= generationStart) {
        cerr << "      - Tracing rays: " << fixed << setprecision(2)
             << (duration_cast<milliseconds>(lastFinishedRay - generationStart)
                     .count() /
                 1000.0)
             << " seconds" << endl;
    }

    if (lastFinishedRay >= startTime) {
        cerr << "  >> Estimated cost: $" << fixed << setprecision(2)
             << (LAMBDA_UNIT_COST * numberOfWorkers *
                 ceil(duration_cast<milliseconds>(lastFinishedRay - startTime)
                          .count() /
                      1000.0))
             << endl;
    }

    cerr << endl;
}
