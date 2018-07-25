#ifndef PBRT_MESSAGES_UTILS_H
#define PBRT_MESSAGES_UTILS_H

#include "geometry.h"
#include "pbrt.pb.h"

namespace pbrt {

    protobuf::Point3f to_protobuf(const Point3f & point);
    protobuf::Bounds3f to_protobuf(const Bounds3f & bounds);

}

#endif /* PBRT_MESSAGES_UTILS_H */
