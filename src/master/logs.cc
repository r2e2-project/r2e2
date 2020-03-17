#include <iomanip>

#include "lambda-master.hh"
#include "messages/utils.hh"

using namespace std;
using namespace std::chrono;
using namespace r2t2;
using namespace PollerShortNames;

void LambdaMaster::recordEnqueue(const WorkerId workerId,
                                 const RayBagInfo &info) {
    auto &worker = workers.at(workerId);
    worker.rays.enqueued += info.rayCount;

    treelets[info.treeletId].lastStats.first = true;

    if (info.sampleBag) {
        worker.stats.samples.rays += info.rayCount;
        worker.stats.samples.bytes += info.bagSize;
        worker.stats.samples.count++;

        aggregatedStats.samples.rays += info.rayCount;
        aggregatedStats.samples.bytes += info.bagSize;
        aggregatedStats.samples.count++;

        lastFinishedRay = steady_clock::now();
    } else {
        worker.stats.enqueued.rays += info.rayCount;
        worker.stats.enqueued.bytes += info.bagSize;
        worker.stats.enqueued.count++;

        treeletStats[info.treeletId].enqueued.rays += info.rayCount;
        treeletStats[info.treeletId].enqueued.bytes += info.bagSize;
        treeletStats[info.treeletId].enqueued.count++;

        aggregatedStats.enqueued.rays += info.rayCount;
        aggregatedStats.enqueued.bytes += info.bagSize;
        aggregatedStats.enqueued.count++;
    }
}

void LambdaMaster::recordAssign(const WorkerId workerId,
                                const RayBagInfo &info) {
    auto &worker = workers.at(workerId);
    worker.rays.dequeued += info.rayCount;

    worker.outstandingRayBags.insert(info);
    worker.outstandingBytes += info.bagSize;

    worker.stats.assigned.rays += info.rayCount;
    worker.stats.assigned.bytes += info.bagSize;
    worker.stats.assigned.count++;

    aggregatedStats.assigned.rays += info.rayCount;
    aggregatedStats.assigned.bytes += info.bagSize;
    aggregatedStats.assigned.count++;
}

void LambdaMaster::recordDequeue(const WorkerId workerId,
                                 const RayBagInfo &info) {
    auto &worker = workers.at(workerId);

    worker.outstandingRayBags.erase(info);
    worker.outstandingBytes -= info.bagSize;

    treelets[info.treeletId].lastStats.first = true;

    worker.stats.dequeued.rays += info.rayCount;
    worker.stats.dequeued.bytes += info.bagSize;
    worker.stats.dequeued.count++;

    treeletStats[info.treeletId].dequeued.rays += info.rayCount;
    treeletStats[info.treeletId].dequeued.bytes += info.bagSize;
    treeletStats[info.treeletId].dequeued.count++;

    aggregatedStats.dequeued.rays += info.rayCount;
    aggregatedStats.dequeued.bytes += info.bagSize;
    aggregatedStats.dequeued.count++;
}

ResultType LambdaMaster::handleWorkerStats() {
    ScopeTimer<TimeLog::Category::WorkerStats> timer_;

    workerStatsWriteTimer.read_event();

    const auto t =
        duration_cast<milliseconds>(steady_clock::now() - startTime).count();

    const float T = static_cast<float>(config.workerStatsWriteInterval);

    for (Worker &worker : workers) {
        if (!worker.isLogged) continue;
        if (worker.state == Worker::State::Terminated) worker.isLogged = false;

        const auto stats = worker.stats - worker.lastStats;
        worker.lastStats = worker.stats;

        /* timestamp,workerId,pathsFinished,
        raysEnqueued,raysAssigned,raysDequeued,
        bytesEnqueued,bytesAssigned,bytesDequeued,
        bagsEnqueued,bagsAssigned,bagsDequeued,
        numSamples,bytesSamples,bagsSamples,cpuUsage */

        wsStream << t << ',' << worker.id << ',' << fixed
                 << (stats.finishedPaths / T) << ','
                 << (stats.enqueued.rays / T) << ','
                 << (stats.assigned.rays / T) << ','
                 << (stats.dequeued.rays / T) << ','
                 << (stats.enqueued.bytes / T) << ','
                 << (stats.assigned.bytes / T) << ','
                 << (stats.dequeued.bytes / T) << ','
                 << (stats.enqueued.count / T) << ','
                 << (stats.assigned.count / T) << ','
                 << (stats.dequeued.count / T) << ','
                 << (stats.samples.rays / T) << ',' << (stats.samples.bytes / T)
                 << ',' << (stats.samples.count / T) << ',' << fixed
                 << setprecision(2) << (100 * stats.cpuUsage) << '\n';
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
           bytesDequeued,bagsEnqueued,bagsDequeued */
        tlStream << t << ',' << treeletId << ',' << fixed
                 << (stats.enqueued.rays / T) << ','
                 << (stats.dequeued.rays / T) << ','
                 << (stats.enqueued.bytes / T) << ','
                 << (stats.dequeued.bytes / T) << ','
                 << (stats.enqueued.count / T) << ','
                 << (stats.dequeued.count / T) << '\n';
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
            ? (10 * aggregatedStats.samples.rays / maxWorkers / totalTime)
            : 0;

    const double estimatedCost =
        LAMBDA_UNIT_COST * maxWorkers * ceil(totalTime);

    proto.set_total_time(totalTime);
    proto.set_generation_time(generationTime);
    proto.set_tracing_time(rayTime);
    proto.set_num_lambdas(maxWorkers);
    proto.set_total_paths(scene.totalPaths);
    proto.set_finished_paths(aggregatedStats.finishedPaths);
    proto.set_finished_rays(aggregatedStats.samples.rays);
    proto.set_num_enqueues(aggregatedStats.enqueued.rays);
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

    cerr << "Job summary:" << endl;
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

    if (aggregatedStats.samples.rays > 0) {
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
         << "N/A" << endl;
}
