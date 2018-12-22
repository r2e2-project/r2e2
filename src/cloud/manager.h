#ifndef PBRT_CLOUD_MANAGER_H
#define PBRT_CLOUD_MANAGER_H

#include <string>

#include "messages/serialization.h"
#include "util/optional.h"
#include "util/util.h"
#include <unordered_map>

namespace pbrt {

class SceneManager {
  public:
    enum class Type {
        Treelet,
        TriangleMesh,
        Lights,
        Sampler,
        Camera,
        Scene,
        Material,
        FloatTexture,
        SpectrumTexture,
        COUNT
    };

    SceneManager() {}

    void init(const std::string& scenePath);
    bool initialized() const { return sceneFD.initialized(); }

    std::unique_ptr<protobuf::RecordReader> GetReader(
        const Type type, const uint32_t id = 0) const;
    std::unique_ptr<protobuf::RecordWriter> GetWriter(
        const Type type, const uint32_t id = 0) const;

    uint32_t getNextId(const Type type, const void* ptr = nullptr) {
        const uint32_t id = autoIds[to_underlying(type)]++;
        if (ptr) {
          ptr_ids[ptr] = id;
        }
        return id;
    }

    uint32_t getId(const void* ptr) {
        if (ptr_ids.count(ptr) == 0) {
            throw std::runtime_error("no id for ptr: " +
                                     std::to_string((uint64_t)ptr));
        }
        return ptr_ids[ptr];
    }

  private:
    static std::string getFileName(const Type type, const uint32_t id);

    size_t autoIds[to_underlying(Type::COUNT)] = {0};
    Optional<FileDescriptor> sceneFD{};
    std::unordered_map<const void*, uint32_t> ptr_ids;
};

namespace global {
extern SceneManager manager;
}

}  // namespace pbrt

#endif /* PBRT_CLOUD_MANAGER_H */

