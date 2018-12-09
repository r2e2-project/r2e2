#ifndef PBRT_CLOUD_MANAGER_H
#define PBRT_CLOUD_MANAGER_H

#include <string>

#include "messages/serialization.h"
#include "util/optional.h"

namespace pbrt {

class SceneManager {
  public:
    enum class Type { Treelet, TriangleMesh, Lights, Sampler, Camera };

    SceneManager() {}

    void init(const std::string& scenePath);
    bool initialized() const { return sceneFD.initialized(); }

    std::unique_ptr<protobuf::RecordReader> GetReader(
        const Type type, const uint32_t id = 0) const;
    std::unique_ptr<protobuf::RecordWriter> GetWriter(
        const Type type, const uint32_t id = 0) const;

  private:
    static std::string getFileName(const Type type, const uint32_t id);

    Optional<FileDescriptor> sceneFD{};
};

namespace global {
extern SceneManager manager;
}

}  // namespace pbrt

#endif /* PBRT_CLOUD_MANAGER_H */
