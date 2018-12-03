#include "serialization.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "util/exception.h"

namespace pbrt {
namespace protobuf {

RecordWriter::RecordWriter(const std::string & filename)
    : fd_(CheckSystemCall(filename, open(filename.c_str(), O_WRONLY | O_CREAT,
                                         S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP |
                                         S_IROTH | S_IWOTH))) {}

void RecordWriter::write_empty() {
    coded_output_.WriteLittleEndian32(0);
}

RecordReader::RecordReader(const std::string & filename)
    : fin_(CheckSystemCall(filename, open(filename.c_str(), O_RDONLY, 0))) {
    coded_input_.SetTotalBytesLimit( 536'870'912, 268'435'456 );
    eof_ = not coded_input_.ReadLittleEndian32(&next_size_);
}

}
}
