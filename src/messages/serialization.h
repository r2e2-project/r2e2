#ifndef PBRT_MESSAGES_SERIALIZATION_H
#define PBRT_MESSAGES_SERIALIZATION_H

#include <string>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>

#include "util/file_descriptor.h"
#include "util/optional.h"
#include "pbrt.pb.h"

namespace pbrt {
namespace protobuf {

class RecordWriter {
public:
    RecordWriter(RecordWriter &&) = default;
    RecordWriter& operator=(RecordWriter &&) = default;

    RecordWriter(const std::string & filename);
    RecordWriter(FileDescriptor && fd);
    RecordWriter(std::ostringstream * os);

    template<class ProtobufType>
    void write(const ProtobufType & proto);

    void write_empty();

private:
    Optional<FileDescriptor> fd_{};

    std::unique_ptr<google::protobuf::io::ZeroCopyOutputStream> output_stream_;
    google::protobuf::io::CodedOutputStream coded_output_;
};

class RecordReader {
public:
    RecordReader(RecordReader &&) = default;
    RecordReader& operator=(RecordReader &&) = default;

    RecordReader(const std::string & filename);
    RecordReader(FileDescriptor && fd);
    RecordReader(std::istringstream && is);

    template<class ProtobufType>
    bool read(ProtobufType * record);
    bool eof() const { return eof_; }

private:
    void initialize();

    Optional<FileDescriptor> fd_;
    Optional<std::istringstream> istream_;

    std::unique_ptr<google::protobuf::io::ZeroCopyInputStream> input_stream_;
    google::protobuf::io::CodedInputStream coded_input_;

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
    if (eof_) { throw std::runtime_error("RecordReader: end of file reached"); }

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
