#if defined(_MSC_VER)
#define NOMINMAX
#pragma once
#endif

#ifndef PBRT_CORE_API_MAKEFNS_H
#define PBRT_CORE_API_MAKEFNS_H

// core/api.h*
#include "pbrt.h"

namespace pbrt {

std::shared_ptr<Material> MakeMaterial(const std::string &name,
                                       const TextureParams &mp);
std::shared_ptr<Texture<Float>> MakeFloatTexture(const std::string &name,
                                                 const Transform &tex2world,
                                                 const TextureParams &tp);
std::shared_ptr<Texture<Spectrum>> MakeSpectrumTexture(
    const std::string &name, const Transform &tex2world,
    const TextureParams &tp);

}  // namespace pbrt

#endif  // PBRT_CORE_API_MAKEFNS_H
