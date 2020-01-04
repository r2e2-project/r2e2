#include "cloud/lambda-master.h"

#include <iomanip>

#include "messages/utils.h"

using namespace std;
using namespace std::chrono;
using namespace pbrt;
using namespace PollerShortNames;

void LambdaMaster::recordEnqueue(const WorkerId workerId,
                                 const RayBagInfo &info) {
    auto &worker = workers.at(workerId);

    worker.lastStats.first = true;
    treelets[info.treeletId].lastStats.first = true;

    if (info.sampleBag) {
        worker.stats.samples.count += info.rayCount;
        worker.stats.samples.bytes += info.bagSize;
        aggregatedStats.samples.count += info.rayCount;
        aggregatedStats.samples.bytes += info.bagSize;

        lastFinishedRay = steady_clock::now();
    } else {
        worker.stats.enqueued.count += info.rayCount;
        worker.stats.enqueued.bytes += info.bagSize;
        treeletStats[info.treeletId].enqueued.count += info.rayCount;
        treeletStats[info.treeletId].enqueued.bytes += info.bagSize;
        aggregatedStats.enqueued.count += info.rayCount;
        aggregatedStats.enqueued.bytes += info.bagSize;
    }
}

void LambdaMaster::recordAssign(const WorkerId workerId,
                                const RayBagInfo &info) {
    auto &worker = workers.at(workerId);

    worker.outstandingRayBags.insert(info);

    worker.lastStats.first = true;
    worker.stats.assigned.count += info.rayCount;
    worker.stats.assigned.bytes += info.bagSize;

    aggregatedStats.assigned.count += info.rayCount;
    aggregatedStats.assigned.bytes += info.bagSize;
}

void LambdaMaster::recordDequeue(const WorkerId workerId,
                                 const RayBagInfo &info) {
    auto &worker = workers.at(workerId);

    worker.outstandingRayBags.erase(info);
    worker.lastStats.first = true;

    treelets[info.treeletId].lastStats.first = true;

    worker.stats.dequeued.count += info.rayCount;
    worker.stats.dequeued.bytes += info.bagSize;
    treeletStats[info.treeletId].dequeued.count += info.rayCount;
    treeletStats[info.treeletId].dequeued.bytes += info.bagSize;
    aggregatedStats.dequeued.count += info.rayCount;
    aggregatedStats.dequeued.bytes += info.bagSize;
}

ResultType LambdaMaster::handleWorkerStats() {
    workerStatsWriteTimer.reset();

    const auto t =
        duration_cast<milliseconds>(steady_clock::now() - startTime).count();

    const float T = static_cast<float>(config.workerStatsWriteInterval);

    for (auto &workerkv : workers) {
        auto &worker = workerkv.second;

        if (!worker.lastStats.first) continue; /* nothing new to log */

        const WorkerStats stats = worker.stats - worker.lastStats.second;
        worker.lastStats.second = worker.stats;
        worker.lastStats.first = false;

        /* timestamp,workerId,pathsFinished,raysEnqueued,raysAssigned,
        raysDequeued,bytesEnqueued,bytesAssigned,bytesDequeued,numSamples,
        bytesSamples */
        wsStream << t << ',' << worker.id << ',' << fixed
                 << (stats.finishedPaths / T) << ','
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
            treeletStats[treeletId] - treelets[treeletId].lastStats.second;

        treelets[treeletId].lastStats.second = treeletStats[treeletId];
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

    double generationTime =
        duration_cast<milliseconds>(lastGeneratorDone - startTime).count() /
        1000.0;

    generationTime = (generationTime < 0) ? 0 : generationTime;

    double rayTime =
        duration_cast<milliseconds>(lastFinishedRay - lastGeneratorDone)
            .count() /
        1000.0;

    rayTime = (rayTime < 0) ? 0 : rayTime;

    const double totalTime = rayTime + generationTime;

    const double avgRayThroughput =
        (totalTime > 0)
            ? (10 * aggregatedStats.samples.count / maxWorkers / totalTime)
            : 0;

    const double estimatedCost =
        LAMBDA_UNIT_COST * maxWorkers * ceil(totalTime);

    proto.set_total_time(totalTime);
    proto.set_generation_time(generationTime);
    proto.set_tracing_time(rayTime);
    proto.set_num_lambdas(maxWorkers);
    proto.set_total_paths(scene.totalPaths);
    proto.set_finished_paths(aggregatedStats.finishedPaths);
    proto.set_finished_rays(aggregatedStats.samples.count);
    proto.set_num_enqueues(aggregatedStats.enqueued.count);
    proto.set_ray_throughput(avgRayThroughput);
    proto.set_total_upload(aggregatedStats.enqueued.bytes);
    proto.set_total_download(aggregatedStats.dequeued.bytes);
    proto.set_total_samples(aggregatedStats.samples.bytes);
    proto.set_estimated_cost(0.0);

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
         << "    Camera rays        " << Value<double>(proto.generation_time())
         << " seconds\n"
         << "    Ray tracing        " << Value<double>(proto.tracing_time())
         << " seconds" << endl;

    cerr << "  Estimated cost       "
         << "N/A" << endl
         << endl;
}
