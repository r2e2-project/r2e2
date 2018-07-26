#ifndef PBRT_MESSAGES_SERIALIZATION_H
#define PBRT_MESSAGES_SERIALIZATION_H

#include <string>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>

#include "util/file_descriptor.h"
#include "pbrt.pb.h"

namespace pbrt {
namespace protobuf {

class RecordWriter {
public:
    RecordWriter(const std::string & filename);

    template<class ProtobufType>
    void write(const ProtobufType & proto);

    void write_empty();

private:
    FileDescriptor fd_;
    google::protobuf::io::FileOutputStream raw_output_ { fd_.fd_num() };
    google::protobuf::io::CodedOutputStream coded_output_ { &raw_output_ };
};

template<class ProtobufType>
void RecordWriter::write(const ProtobufType & proto) {
    coded_output_.WriteLittleEndian32(proto.ByteSize());
    if (not proto.SerializeToCodedStream(&coded_output_)) {
        throw std::runtime_error("write: write protobuf error");
    }
}

}
}


#endif /* PBRT_MESSAGES_SERIALIZATION_H */
