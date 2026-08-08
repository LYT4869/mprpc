add_executable(async_rpc_test
    integration/async_rpc_test.cc
    ${PROJECT_SOURCE_DIR}/example/friend.pb.cc 
)

target_link_libraries(async_rpc_test
    PRIVATE
    mprpc
    protobuf
    pthread
)