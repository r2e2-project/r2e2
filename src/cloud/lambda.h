#ifndef PBRT_CLOUD_LAMBDA_H
#define PBRT_CLOUD_LAMBDA_H

#include <cstdint>
#include <sstream>
#include <string>

using WorkerId = uint64_t;
using TreeletId = uint32_t;
using BagId = uint64_t;

struct RayBagKey {
    WorkerId workerId;
    TreeletId treeletId;
    BagId bagId;
    size_t size;

    std::string str(const std::string& prefix) const {
        std::ostringstream oss;
        oss << prefix << workerId << '-' << treeletId << '-' << bagId;
        return oss.str();
    }
};

struct RayBag {
    RayBagKey key;
    std::string data;

    RayBag(const RayBagKey& key, std::string&& data)
        : key(key), data(std::move(data)) {}
};

#endif /* PBRT_CLOUD_LAMBDA_H */
