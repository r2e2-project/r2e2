#ifndef PBRT_SHAPES_FAKE_H
#define PBRT_SHAPES_FAKE_H

// shapes/cone.h*
#include "shape.h"

namespace pbrt {

extern Transform fakeShapeIdentityTransform;

// Cone Declarations
class FakeShape : public Shape {
  public:
    // Cone Public Methods
    FakeShape(const Bounds3f &worldBound)
        : Shape(&fakeShapeIdentityTransform, &fakeShapeIdentityTransform,
                false),
          worldBound(worldBound) {}
    ~FakeShape() {}

    Bounds3f ObjectBound() const { return worldBound; }
    Bounds3f WorldBound() const { return worldBound; }

    bool Intersect(const Ray &, Float *, SurfaceInteraction *,
                   bool = true) const {
        return false;
    }
    bool IntersectP(const Ray &, bool = true) const { return false; }
    Float Area() const { return 0.f; }
    Interaction Sample(const Point2f &, Float *) const { return {}; }
    Float Pdf(const Interaction &) const { return 0.f; }
    Float Pdf(const Interaction &, const Vector3f &) const { return 0.f; }
    Interaction Sample(const Interaction &, const Point2f &, Float *) const {
        return {};
    }

    virtual Float SolidAngle(const Point3f &, int = 512) const { return 0.f; }

    ShapeType GetType() const { return ShapeType::Fake; }

  private:
    const Bounds3f worldBound;
};

}  // namespace pbrt

#endif  // PBRT_SHAPES_FAKE_H
