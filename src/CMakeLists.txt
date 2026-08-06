aux_source_directory(. SRC_LIST)
aux_source_directory(./proto PROTO_LIST)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_library(mprpc ${SRC_LIST} ${PROTO_LIST})
target_compile_definitions(mprpc PRIVATE THREADED)
target_link_libraries(mprpc muduo_net muduo_base  pthread zookeeper_mt protobuf)