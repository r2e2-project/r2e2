#ifndef PBRT_CLOUD_MANAGER_H
#define PBRT_CLOUD_MANAGER_H

#include <string>

#include <unordered_map>
#include "messages/serialization.h"
#include "util/optional.h"
#include "util/util.h"

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

    using ReaderPtr = std::unique_ptr<protobuf::RecordReader>;
    using WriterPtr = std::unique_ptr<protobuf::RecordWriter>;

    void init(const std::string& scenePath);
    bool initialized() const { return sceneFD.initialized(); }
    ReaderPtr GetReader(const Type type, const uint32_t id = 0) const;
    WriterPtr GetWriter(const Type type, const uint32_t id = 0) const;
    uint32_t getNextId(const Type type, const void* ptr = nullptr);
    uint32_t getId(const void* ptr) const { return ptrIds.at(ptr); }

  private:
    static std::string getFileName(const Type type, const uint32_t id);

    size_t autoIds[to_underlying(Type::COUNT)] = {0};
    Optional<FileDescriptor> sceneFD{};
    std::unordered_map<const void*, uint32_t> ptrIds;
};

namespace global {
extern SceneManager manager;
}

}  // namespace pbrt

#endif /* PBRT_CLOUD_MANAGER_H */
