#include "stats.h"

#include <cmath>
#include <cstring>
#include <iomanip>

using namespace std;
using namespace chrono;

namespace pbrt {

void TreeletStats::merge(const TreeletStats& other) {
    enqueued.rays += other.enqueued.rays;
    dequeued.rays += other.dequeued.rays;
    enqueued.bytes += other.enqueued.bytes;
    dequeued.bytes += other.dequeued.bytes;
    enqueued.count += other.enqueued.count;
    dequeued.count += other.dequeued.count;
}

void TreeletStats::reset() { *this = {}; }

TreeletStats TreeletStats::operator-(const TreeletStats& other) const {
    TreeletStats res;

    res.enqueued.rays = enqueued.rays - other.enqueued.rays;
    res.dequeued.rays = dequeued.rays - other.dequeued.rays;
    res.enqueued.bytes = enqueued.bytes - other.enqueued.bytes;
    res.dequeued.bytes = dequeued.bytes - other.dequeued.bytes;
    res.enqueued.count = enqueued.count - other.enqueued.count;
    res.dequeued.count = dequeued.count - other.dequeued.count;

    return res;
}

void WorkerStats::merge(const WorkerStats& other) {
    finishedPaths += other.finishedPaths;
    enqueued.rays += other.enqueued.rays;
    assigned.rays += other.assigned.rays;
    dequeued.rays += other.dequeued.rays;
    samples.rays += other.samples.rays;
    enqueued.bytes += other.enqueued.bytes;
    assigned.bytes += other.assigned.bytes;
    dequeued.bytes += other.dequeued.bytes;
    samples.bytes += other.samples.bytes;
    enqueued.count += other.enqueued.count;
    assigned.count += other.assigned.count;
    dequeued.count += other.dequeued.count;
    samples.count += other.samples.count;
}

void WorkerStats::reset() { *this = {}; }

WorkerStats WorkerStats::operator-(const WorkerStats& other) const {
    WorkerStats res;

    res.finishedPaths = finishedPaths - other.finishedPaths;
    res.enqueued.rays = enqueued.rays - other.enqueued.rays;
    res.assigned.rays = assigned.rays - other.assigned.rays;
    res.dequeued.rays = dequeued.rays - other.dequeued.rays;
    res.samples.rays = samples.rays - other.samples.rays;
    res.enqueued.bytes = enqueued.bytes - other.enqueued.bytes;
    res.assigned.bytes = assigned.bytes - other.assigned.bytes;
    res.dequeued.bytes = dequeued.bytes - other.dequeued.bytes;
    res.samples.bytes = samples.bytes - other.samples.bytes;
    res.enqueued.count = enqueued.count - other.enqueued.count;
    res.assigned.count = assigned.count - other.assigned.count;
    res.dequeued.count = dequeued.count - other.dequeued.count;
    res.samples.count = samples.count - other.samples.count;

    return res;
}

}  // namespace pbrt
