#ifndef PBRT_CLOUD_PRIMITIVE_H
#define PBRT_CLOUD_PRIMITIVE_H

#include "core/primitive.h"

namespace pbrt {

class CloudPrimitive : public Primitive {
  public:
    CloudPrimitive(const uint32_t primitive);
};

}  // namespace pbrt

#endif /* PBRT_CLOUD_PRIMITIVE_H */
