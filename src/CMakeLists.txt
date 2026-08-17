aux_source_directory(. SRC_LIST)
aux_source_directory(./proto PROTO_LIST)

list(REMOVE_ITEM SRC_LIST ${CMAKE_CURRENT_SOURCE_DIR}/boundedexecutor.cc)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_library(mprpc_executor boundedexecutor.cc)
target_include_directories(mprpc_executor PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(mprpc_executor PUBLIC pthread)

add_library(mprpc ${SRC_LIST} ${PROTO_LIST})
target_compile_definitions(mprpc PRIVATE THREADED)
target_link_libraries(mprpc muduo_net muduo_base pthread zookeeper_mt protobuf ZLIB::ZLIB mprpc_executor)
