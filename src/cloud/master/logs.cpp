#include "cloud/lambda-master.h"

#include <iomanip>

#include "messages/utils.h"

using namespace std;
using namespace std::chrono;
using namespace pbrt;
using namespace PollerShortNames;

void LambdaMaster::logEnqueue(const WorkerId workerId, const RayBagInfo &info) {
    auto &worker = workerStats[workerId];
    auto &treelet = treeletStats[info.treeletId];

    lastStats.workers[workerId].second = true;
    lastStats.treelets[info.treeletId].second = true;

    if (info.sampleBag) {
        worker.samples.count += info.rayCount;
        worker.samples.bytes += info.bagSize;

        lastFinishedRay = steady_clock::now();
    } else {
        worker.enqueued.count += info.rayCount;
        worker.enqueued.bytes += info.bagSize;
        treelet.enqueued.count += info.rayCount;
        treelet.enqueued.bytes += info.bagSize;
    }
}

void LambdaMaster::logDequeue(const WorkerId workerId, const RayBagInfo &info) {
    auto &worker = workerStats[workerId];
    auto &treelet = treeletStats[info.treeletId];

    lastStats.workers[workerId].second = true;
    lastStats.treelets[info.treeletId].second = true;

    worker.dequeued.count += info.rayCount;
    worker.dequeued.bytes += info.bagSize;
    treelet.dequeued.count += info.rayCount;
    treelet.dequeued.bytes += info.bagSize;
}

ResultType LambdaMaster::handleWorkerStats() {
    workerStatsWriteTimer.reset();

    const auto t =
        duration_cast<milliseconds>(steady_clock::now() - startTime).count();

    const float T = static_cast<float>(config.workerStatsWriteInterval);

    for (size_t workerId = 1; workerId <= numberOfLambdas; workerId++) {
        if (!lastStats.workers[workerId].second) {
            continue; /* nothing new to log */
        }

        const WorkerStats stats =
            workerStats[workerId] - lastStats.workers[workerId].first;

        lastStats.workers[workerId].first = workerStats[workerId];
        lastStats.workers[workerId].second = false;

        /* timestamp,workerId,raysEnqueued,raysDequeued,bytesEnqueued,
           bytesDequeued,numSamples,bytesSamples */
        wsStream << t << ',' << workerId << ',' << fixed
                 << (stats.enqueued.count / T) << ','
                 << (stats.dequeued.count / T) << ','
                 << (stats.enqueued.bytes / T) << ','
                 << (stats.dequeued.bytes / T) << ','
                 << (stats.samples.count / T) << ','
                 << (stats.samples.bytes / T) << '\n';
    }

    for (size_t treeletId = 0; treeletId < treeletStats.size(); treeletId++) {
        if (!lastStats.treelets[treeletId].second) {
            continue; /* nothing new to log */
        }

        const TreeletStats stats =
            treeletStats[treeletId] - lastStats.treelets[treeletId].first;

        lastStats.treelets[treeletId].first = treeletStats[treeletId];
        lastStats.treelets[treeletId].second = false;

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

    proto.set_num_lambdas(numberOfLambdas);
    proto.set_total_paths(scene.totalPaths);
    proto.set_finished_paths(0);
    proto.set_finished_rays(0);

    ofstream fout{config.jobSummaryPath};
    fout << protoutil::to_json(proto) << endl;
}

void LambdaMaster::printJobSummary() const {
    const static double LAMBDA_UNIT_COST = 0.00004897; /* $/lambda/sec */

    cerr << "* Job summary: " << endl;
    cerr << "  >> Average ray throughput: "
         << (1.0 * 0 / numberOfLambdas /
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
             << (LAMBDA_UNIT_COST * numberOfLambdas *
                 ceil(duration_cast<milliseconds>(lastFinishedRay - startTime)
                          .count() /
                      1000.0))
             << endl;
    }

    cerr << endl;
}
