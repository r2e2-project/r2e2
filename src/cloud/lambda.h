#ifndef PBRT_CLOUD_LAMBDA_H
#define PBRT_CLOUD_LAMBDA_H

#include <cstdint>
#include <sstream>
#include <string>

using WorkerId = uint64_t;
using TreeletId = uint32_t;
using BagId = uint64_t;

struct RayBagInfo {
    WorkerId workerId{};
    TreeletId treeletId{};
    BagId bagId{};
    size_t rayCount{};
    size_t bagSize{};
    bool finishedRays{false};

    std::string str(const std::string& prefix) const {
        std::ostringstream oss;

        if (!finishedRays) {
            oss << prefix << 'T' << treeletId << '-' << workerId << '_'
                << bagId;
        } else {
            oss << prefix << "finished-" << workerId << "_" << bagId;
        }

        return oss.str();
    }

    RayBagInfo(const WorkerId workerId, const TreeletId treeletId,
               const BagId bagId, const size_t rayCount, const size_t bagSize,
               const bool finishedRays)
        : workerId(workerId),
          treeletId(treeletId),
          bagId(bagId),
          rayCount(rayCount),
          bagSize(bagSize),
          finishedRays(finishedRays) {}

    RayBagInfo() = default;
    RayBagInfo(const RayBagInfo&) = default;
    RayBagInfo& operator=(const RayBagInfo&) = default;
};

struct RayBag {
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
