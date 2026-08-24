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

add_executable(codec_test codec_test.cpp)
target_link_libraries(codec_test PRIVATE mprpc)

add_executable(rpcmetrics_test rpcmetrics_test.cpp)
target_link_libraries(rpcmetrics_test PRIVATE mprpc)

add_executable(provider_deadline_test
    integration/provider_deadline_test.cc
    ${PROJECT_SOURCE_DIR}/example/friend.pb.cc
)
target_link_libraries(provider_deadline_test
    PRIVATE mprpc protobuf pthread)

if(BUILD_TESTING)
    add_test(NAME codec_test COMMAND codec_test)
    add_test(NAME rpcmetrics_test COMMAND rpcmetrics_test)
endif()
