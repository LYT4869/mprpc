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

add_executable(rpcclientruntime_test rpcclientruntime_test.cpp)
target_link_libraries(rpcclientruntime_test PRIVATE mprpc)

add_executable(rpcdispatchstate_test rpcdispatchstate_test.cpp)
target_link_libraries(rpcdispatchstate_test PRIVATE mprpc)

add_executable(boundedexecutor_test boundedexecutor_test.cpp)
target_link_libraries(boundedexecutor_test PRIVATE mprpc_executor pthread)

add_executable(callbackadmission_test callbackadmission_test.cpp)
target_link_libraries(callbackadmission_test PRIVATE mprpc)

add_executable(provider_deadline_test
    integration/provider_deadline_test.cc
    ${PROJECT_SOURCE_DIR}/example/friend.pb.cc
)
target_link_libraries(provider_deadline_test
    PRIVATE mprpc protobuf pthread)

add_executable(provider_executor_test
    integration/provider_executor_test.cc
    ${PROJECT_SOURCE_DIR}/example/friend.pb.cc
)
target_link_libraries(provider_executor_test
    PRIVATE mprpc protobuf pthread)

add_executable(zk_child_watch_test
    integration/zk_child_watch_test.cc
)
target_link_libraries(zk_child_watch_test PRIVATE mprpc pthread)

add_executable(discovery_provider
    integration/discovery_provider.cc
    ${PROJECT_SOURCE_DIR}/example/friend.pb.cc
)
target_link_libraries(discovery_provider PRIVATE mprpc protobuf pthread)

add_executable(discovery_watch_test
    integration/discovery_watch_test.cc
    ${PROJECT_SOURCE_DIR}/example/friend.pb.cc
)
target_link_libraries(discovery_watch_test PRIVATE mprpc protobuf pthread)

add_executable(provider_reregistration_test
    integration/provider_reregistration_test.cc
    ${PROJECT_SOURCE_DIR}/example/friend.pb.cc
)
target_link_libraries(provider_reregistration_test
    PRIVATE mprpc protobuf pthread)

if(BUILD_TESTING)
    add_test(NAME codec_test COMMAND codec_test)
    add_test(NAME rpcmetrics_test COMMAND rpcmetrics_test)
    add_test(NAME rpcclientruntime_test COMMAND rpcclientruntime_test)
    add_test(NAME rpcdispatchstate_test COMMAND rpcdispatchstate_test)
    add_test(NAME boundedexecutor_test COMMAND boundedexecutor_test)
    add_test(NAME callbackadmission_test COMMAND callbackadmission_test)
endif()
