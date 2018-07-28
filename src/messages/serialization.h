#ifndef PBRT_MESSAGES_SERIALIZATION_H
#define PBRT_MESSAGES_SERIALIZATION_H

#include <string>
#include <iostream>
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

class RecordReader {
public:
    RecordReader(const std::string & filename);

    template<class ProtobufType>
    bool read(ProtobufType * record);
    bool eof() const { return eof_; }

private:
    FileDescriptor fin_;
    google::protobuf::io::FileInputStream raw_input_ { fin_.fd_num() };
    google::protobuf::io::CodedInputStream coded_input_ { &raw_input_ };

    google::protobuf::uint32 next_size_;
    bool eof_ {false};
};

template<class ProtobufType>
void RecordWriter::write(const ProtobufType & proto) {
    coded_output_.WriteLittleEndian32(proto.ByteSize());
    if (not proto.SerializeToCodedStream(&coded_output_)) {
        throw std::runtime_error("write: write protobuf error");
    }
}

template<class ProtobufType>
bool RecordReader::read(ProtobufType * record) {
    if (eof_) { return false; }

    if (next_size_ == 0) {
        eof_ = not coded_input_.ReadLittleEndian32(&next_size_);
        return false;
    }

    google::protobuf::io::CodedInputStream::Limit message_limit =
        coded_input_.PushLimit(next_size_);

    if (record->ParseFromCodedStream(&coded_input_)) {
        coded_input_.PopLimit(message_limit);
        eof_ = not coded_input_.ReadLittleEndian32(&next_size_);
        return true;
    }

    eof_ = true;
    return false;
}

}
}


#endif /* PBRT_MESSAGES_SERIALIZATION_H */
