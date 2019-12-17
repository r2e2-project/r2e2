#ifndef PBRT_CLOUD_LAMBDA_H
#define PBRT_CLOUD_LAMBDA_H

#include <cstdint>

using WorkerId = uint64_t;
using TreeletId = uint32_t;
using BagId = uint64_t;

struct RayBag {
    WorkerId workerId;
    TreeletId treeletId;
    BagId bagId;
    size_t size;
};

#endif /* PBRT_CLOUD_LAMBDA_H */
