# Dockerfile for building an Amazon Linux 2 image for compiling the static
# binary for R2E2 worker

FROM amazonlinux:2

MAINTAINER sadjad "https://github.com/sadjad"

RUN yum update -y
RUN yum install -y tar gzip cmake3 gcc10-gcc gcc10-c++ pkgconfig \
                   gperftools-devel openssl11-devel openssl11-static \
                   lz4-devel lz4-static glibc-static zlib-devel zlib-static  \
                   libatomic10-devel
                   

RUN ln -s $(command -v gcc10-gcc) /usr/bin/gcc
RUN ln -s $(command -v gcc10-g++) /usr/bin/g++
RUN ln -s $(command -v gcc10-ar) /usr/bin/ar

# Build and install liblzma
WORKDIR /root
RUN curl -L https://tukaani.org/xz/xz-5.2.5.tar.gz --output - | tar xzf -
WORKDIR /root/xz-5.2.5
RUN ./configure --prefix=/usr --enable-static
RUN make -j$(nproc)
RUN make install

# Build and install libunwind
WORKDIR /root
RUN curl -L https://github.com/libunwind/libunwind/releases/download/v1.6.2/libunwind-1.6.2.tar.gz --output - | tar xzf -
WORKDIR /root/libunwind-1.6.2
RUN ./configure --prefix=/usr --enable-static
RUN make -j$(nproc)
RUN make install

# Build and install protobuf
WORKDIR /root
RUN curl -L https://github.com/protocolbuffers/protobuf/archive/refs/tags/v3.12.4.tar.gz --output - | tar xzf -
WORKDIR /root/protobuf-3.12.4/cmake
RUN mkdir build
WORKDIR /root/protobuf-3.12.4/cmake/build
RUN cmake3 -DCMAKE_BUILD_TYPE=Release -Dprotobuf_BUILD_TESTS=OFF \
           -DCMAKE_INSTALL_PREFIX=/usr ..
RUN make -j$(nproc)
RUN make install
