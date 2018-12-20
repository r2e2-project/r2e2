#ifndef PBRT_CLOUD_MANAGER_H
#define PBRT_CLOUD_MANAGER_H

#include <string>

#include "messages/serialization.h"
#include "util/optional.h"
#include "util/util.hh"

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
        COUNT
    };

    SceneManager() {}

    void init(const std::string& scenePath);
    bool initialized() const { return sceneFD.initialized(); }

    std::unique_ptr<protobuf::RecordReader> GetReader(
        const Type type, const uint32_t id = 0) const;
    std::unique_ptr<protobuf::RecordWriter> GetWriter(
        const Type type, const uint32_t id = 0) const;

    uint32_t getNextId(const Type type) {
        return autoIds[to_underlying(type)]++;
    }

  private:
    static std::string getFileName(const Type type, const uint32_t id);

    size_t autoIds[to_underlying(Type::COUNT)] = {0};
    Optional<FileDescriptor> sceneFD{};
};

namespace global {
extern SceneManager manager;
}

}  // namespace pbrt

#endif /* PBRT_CLOUD_MANAGER_H */
