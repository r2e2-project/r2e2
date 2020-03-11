#ifndef PBRT_CLOUD_LAMBDA_H
#define PBRT_CLOUD_LAMBDA_H

#include <chrono>
#include <cstdint>
#include <sstream>
#include <string>

constexpr size_t WORKER_MAX_ACTIVE_RAYS = 100'000; /* ~120 MiB of rays */

using WorkerId = uint64_t;
using TreeletId = uint32_t;
using BagId = uint64_t;

struct RayBagInfo {
    bool tracked{false};

    WorkerId workerId{};
    TreeletId treeletId{};
    BagId bagId{};
    size_t rayCount{};
    size_t bagSize{};
    bool sampleBag{false};

    std::string str(const std::string& prefix) const {
        std::ostringstream oss;

        if (!sampleBag) {
            oss << prefix << "T" << treeletId << "/W" << workerId << "/B"
                << bagId;
        } else {
            oss << prefix << "samples/W" << workerId << "/B" << bagId;
        }

        return oss.str();
    }

    RayBagInfo(const WorkerId workerId, const TreeletId treeletId,
               const BagId bagId, const size_t rayCount, const size_t bagSize,
               const bool sampleBag)
        : workerId(workerId),
          treeletId(treeletId),
          bagId(bagId),
          rayCount(rayCount),
          bagSize(bagSize),
          sampleBag(sampleBag) {}

    RayBagInfo() = default;
    RayBagInfo(const RayBagInfo&) = default;
    RayBagInfo& operator=(const RayBagInfo&) = default;

    bool operator<(const RayBagInfo& other) const {
        return (workerId < other.workerId) ||
               (workerId == other.workerId &&
                (treeletId < other.treeletId ||
                 (treeletId == other.treeletId && bagId < other.bagId)));
    }

    static RayBagInfo& EmptyBag() {
        static RayBagInfo bag;
        return bag;
    }
};

struct RayBag {
    std::chrono::steady_clock::time_point createdAt{
        std::chrono::steady_clock::now()};

    RayBagInfo info;
    std::string data;

    RayBag(const WorkerId workerId, const TreeletId treeletId,
           const BagId bagId, const bool finished, const size_t maxBagLen)
        : info(workerId, treeletId, bagId, 0, 0, finished),
          data(maxBagLen, '\0') {}

    RayBag(const RayBagInfo& info, std::string&& data)
        : info(info), data(std::move(data)) {}
};

#endif /* PBRT_CLOUD_LAMBDA_H */
