#include "cloud/lambda-master.h"

#include <iomanip>

#include "messages/utils.h"

using namespace std;
using namespace std::chrono;
using namespace pbrt;
using namespace PollerShortNames;

void LambdaMaster::recordEnqueue(const WorkerId workerId,
                                 const RayBagInfo &info) {
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

void LambdaMaster::recordAssign(const WorkerId workerId,
                                const RayBagInfo &info) {
    auto &worker = workers[workerId];

    worker.assignedRayBags.insert(info);

    worker.lastStats.first = true;
    worker.stats.assigned.count += info.rayCount;
    worker.stats.assigned.bytes += info.bagSize;

    aggregatedStats.assigned.count += info.rayCount;
    aggregatedStats.assigned.bytes += info.bagSize;
}

void LambdaMaster::recordDequeue(const WorkerId workerId,
                                 const RayBagInfo &info) {
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

protobuf::JobSummary LambdaMaster::getJobSummary() const {
    protobuf::JobSummary proto;

    constexpr static double LAMBDA_UNIT_COST = 0.00004897; /* $/lambda/sec */

    double launchTime =
        duration_cast<milliseconds>(generationStart - startTime).count() /
        1000.0;

    launchTime = (launchTime < 0) ? 0 : launchTime;

    double rayTime =
        duration_cast<milliseconds>(lastFinishedRay - generationStart).count() /
        1000.0;

    rayTime = (rayTime < 0) ? 0 : rayTime;

    const double totalTime = launchTime + rayTime;

    const double avgRayThroughput =
        (totalTime > 0)
            ? (10 * aggregatedStats.samples.count / numberOfWorkers / totalTime)
            : 0;

    const double estimatedCost =
        LAMBDA_UNIT_COST * numberOfWorkers * ceil(totalTime);

    proto.set_total_time(totalTime);
    proto.set_launch_time(launchTime);
    proto.set_ray_time(rayTime);
    proto.set_num_lambdas(numberOfWorkers);
    proto.set_total_paths(scene.totalPaths);
    proto.set_finished_paths(aggregatedStats.finishedPaths);
    proto.set_finished_rays(aggregatedStats.samples.count);
    proto.set_num_enqueues(aggregatedStats.enqueued.count);
    proto.set_ray_throughput(avgRayThroughput);
    proto.set_total_upload(aggregatedStats.enqueued.bytes);
    proto.set_total_download(aggregatedStats.dequeued.bytes);
    proto.set_total_samples(aggregatedStats.samples.bytes);
    proto.set_estimated_cost(estimatedCost);

    return proto;
}

void LambdaMaster::dumpJobSummary() const {
    protobuf::JobSummary proto = getJobSummary();
    ofstream fout{config.jobSummaryPath};
    fout << protoutil::to_json(proto) << endl;
}

template <class T>
class Value {
  private:
    T value;

  public:
    Value(T value) : value(value) {}
    T get() const { return value; }
};

template <class T>
ostream &operator<<(ostream &o, const Value<T> &v) {
    o << "\e[1m" << v.get() << "\e[0m";
    return o;
}

void LambdaMaster::printJobSummary() const {
    auto percent = [](const uint64_t n, const uint64_t total) -> double {
        return total ? (((uint64_t)(100 * (100.0 * n / total))) / 100.0) : 0.0;
    };

    const protobuf::JobSummary proto = getJobSummary();

    cerr << endl << "Job summary:" << endl;
    cerr << "  Ray throughput       " << fixed << setprecision(2)
         << Value<double>(proto.ray_throughput()) << " rays/worker/s" << endl;

    cerr << "  Total paths          " << Value<uint64_t>(proto.total_paths())
         << endl;

    cerr << "  Finished paths       " << Value<uint64_t>(proto.finished_paths())
         << " (" << fixed << setprecision(2)
         << percent(proto.finished_paths(), proto.total_paths()) << "%)"
         << endl;

    cerr << "  Finished rays        " << Value<uint64_t>(proto.finished_rays())
         << endl;

    cerr << "  Total transfers      " << Value<uint64_t>(proto.num_enqueues());

    if (aggregatedStats.samples.count > 0) {
        cerr << " (" << fixed << setprecision(2)
             << (1.0 * proto.num_enqueues() / proto.finished_rays())
             << " transfers/ray)";
    }

    cerr << endl;

    cerr << "  Total upload         "
         << Value<string>(format_bytes(proto.total_upload())) << endl;

    cerr << "  Total download       "
         << Value<string>(format_bytes(proto.total_download())) << endl;

    cerr << "  Total sample size    "
         << Value<string>(format_bytes(proto.total_samples())) << endl;

    cerr << "  Total time           " << fixed << setprecision(2)
         << Value<double>(proto.total_time()) << " seconds\n"
         << "    Starting workers   " << Value<double>(proto.launch_time())
         << " seconds\n"
         << "    Tracing rays       " << Value<double>(proto.ray_time())
         << " seconds" << endl;

    cerr << "  Estimated cost       "
         << "$" << fixed << setprecision(2)
         << Value<double>(proto.estimated_cost()) << endl
         << endl;
}
