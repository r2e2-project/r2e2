#ifndef PBRT_CLOUD_LAMBDA_H
#define PBRT_CLOUD_LAMBDA_H

#include <cstdint>
#include <sstream>
#include <string>

using WorkerId = uint64_t;
using TreeletId = uint32_t;
using BagId = uint64_t;

struct RayBagKey {
    WorkerId workerId{};
    TreeletId treeletId{};
    BagId bagId{};
    size_t rayCount{};
    size_t bagSize{};

    std::string str(const std::string& prefix) const {
        std::ostringstream oss;
        oss << prefix << 'T' << treeletId << '-' << workerId << '_' << bagId;
        return oss.str();
    }

    RayBagKey(const WorkerId workerId, const TreeletId treeletId,
              const BagId bagId, const size_t rayCount, const size_t bagSize)
        : workerId(workerId),
          treeletId(treeletId),
          bagId(bagId),
          rayCount(rayCount),
          bagSize(bagSize) {}

    RayBagKey() = default;
    RayBagKey(const RayBagKey&) = default;
    RayBagKey& operator=(const RayBagKey&) = default;
};

struct RayBag {
    RayBagKey key;
    std::string data;

    RayBag(const RayBagKey& key, std::string&& data)
        : key(key), data(std::move(data)) {}
};

#endif /* PBRT_CLOUD_LAMBDA_H */
