cmake_minimum_required ( VERSION 2.8.5 )
cmake_policy ( SET CMP0054 NEW )
project ( R2T2 )

add_subdirectory ( lib/pbrt )

include ( etc/build_defs.cmake )
include ( etc/build_type.cmake )
include ( etc/cflags.cmake )

include ( etc/clang_format.cmake )
include ( etc/clang_tidy.cmake )

include ( FindProtobuf )
find_package ( Protobuf REQUIRED )
find_library ( PTHREAD pthread )

include ( FindOpenSSL )

include ( etc/find_lz4.cmake )

include_directories ( SYSTEM
  "${CMAKE_CURRENT_LIST_DIR}/lib/pbrt/src/include"
  "${CMAKE_CURRENT_LIST_DIR}/lib/pbrt/src/core"
)

add_subdirectory ( src/messages )
include_directories ( "${CMAKE_CURRENT_BINARY_DIR}/src/messages" )

add_definitions ( -D PBRT_CONSTEXPR=constexpr )

file ( GLOB R2T2_SOURCE
    #src/benchmark/*
    src/common/*
    src/execution/*
    src/execution/meow/*
    src/master/*
    src/messages/*
    src/net/*
    src/remote/*
    src/schedulers/*
    #src/server/*
    #src/simulator/*
    src/storage/*
    src/tools/*
    src/util/*
    src/worker/*
)

include_directories ( src )

add_library( r2t2 STATIC
    ${R2T2_SOURCE}
)

set ( ALL_R2T2_LIBS
    r2t2
    r2t2_messages
    pbrt
    pbrt_messages
    atomic
    ${CMAKE_THREAD_LIBS_INIT}
    IlmImf
    Imath
    Half
    glog
    Ptex_static
    ${LZ4_LIBRARY}
    ${ZLIB_LIBRARY}
    ${Protobuf_LIBRARIES}
    ${OPENSSL_LIBRARIES}
    cryptopp
)

add_executable ( r2t2-lambda-master src/master/lambda-master.cc )
target_link_libraries( r2t2-lambda-master ${ALL_R2T2_LIBS} )

add_executable ( r2t2-lambda-worker src/worker/lambda-worker.cc )
target_link_libraries( r2t2-lambda-worker ${ALL_R2T2_LIBS} dl z unwind lzma
    -static -Wl,-allow-multiple-definition -s -Wl,--whole-archive
    -lpthread -Wl,--no-whole-archive)

add_executable ( r2t2-lambda-aggregate src/tools/lambda-aggregate.cc )
target_link_libraries( r2t2-lambda-aggregate ${ALL_R2T2_LIBS} )
