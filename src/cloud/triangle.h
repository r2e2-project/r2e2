#ifndef PBRT_CLOUD_TRIANGLE_H
#define PBRT_CLOUD_TRIANGLE_H

#include <memory>

#include "core/primitive.h"
#include "shapes/triangle.h"

class CloudTriangleMesh {
public:
    CloudTriangleMesh(const uint32_t mesh_id);
    std::shared_ptr<TriangleMesh> mesh;
};

class CloudTriangle : public Primitive {
  public:
    CloudTriangle(std::shared_ptr<CloudTriangleMesh> &mesh,
                  const int triNumber);

  private:
    Triangle triangle;
    std::shared_ptr<CloudTriangleMesh> mesh;
};

#endif /* PBRT_CLOUD_TRIANGLE_H */
