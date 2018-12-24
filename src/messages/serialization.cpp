#include "serialization.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "util/exception.h"

using namespace std;
using namespace google::protobuf::io;

namespace pbrt {
namespace protobuf {

RecordWriter::RecordWriter(const string& filename)
    : RecordWriter(FileDescriptor(CheckSystemCall(
          filename,
          open(filename.c_str(), O_WRONLY | O_CREAT | O_TRUNC,
               S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH)))) {}

RecordWriter::RecordWriter(FileDescriptor&& fd)
    : fd_(true, move(fd)),
      output_stream_(make_unique<FileOutputStream>(fd_->fd_num())),
      coded_output_(output_stream_.get()) {}

RecordWriter::RecordWriter(std::ostringstream* os)
    : output_stream_(make_unique<OstreamOutputStream>(os)),
      coded_output_(output_stream_.get()) {}

void RecordWriter::write_empty() { coded_output_.WriteLittleEndian32(0); }

RecordReader::RecordReader(const string& filename)
    : RecordReader(FileDescriptor(
          CheckSystemCall(filename, open(filename.c_str(), O_RDONLY, 0)))) {}

RecordReader::RecordReader(FileDescriptor&& fd)
    : fd_(true, move(fd)),
      input_stream_(make_unique<FileInputStream>(fd_->fd_num())),
      coded_input_(input_stream_.get()) {
    initialize();
}

RecordReader::RecordReader(istringstream&& is)
    : istream_(true, move(is)),
      input_stream_(make_unique<IstreamInputStream>(&*istream_)),
      coded_input_(input_stream_.get()) {
    initialize();
}

void RecordReader::initialize() {
    coded_input_.SetTotalBytesLimit(1'073'741'824, 536'870'912);
    eof_ = not coded_input_.ReadLittleEndian32(&next_size_);
}

}  // namespace protobuf
}  // namespace pbrt
