set(FILE_TRANSFER_PROTO
    ${CMAKE_CURRENT_SOURCE_DIR}/proto/file_transfer.proto
)

set(FILE_TRANSFER_GENERATED_DIR
    ${CMAKE_CURRENT_BINARY_DIR}/generated
)

find_package(
    OpenSSL REQUIRED
    COMPONENTS Crypto
)
find_package(Threads REQUIRED)
find_package(ZLIB REQUIRED)

file(MAKE_DIRECTORY
    ${FILE_TRANSFER_GENERATED_DIR}
)

add_library(mprpc_file_transfer_proto STATIC
    ${FILE_TRANSFER_PROTO}
)

protobuf_generate(
    TARGET mprpc_file_transfer_proto
    LANGUAGE cpp
    PROTOC_OUT_DIR ${FILE_TRANSFER_GENERATED_DIR}
    APPEND_PATH
)

target_include_directories(
    mprpc_file_transfer_proto
    PUBLIC
    ${FILE_TRANSFER_GENERATED_DIR}
)

target_link_libraries(
    mprpc_file_transfer_proto
    PUBLIC
    protobuf::libprotobuf
)

target_compile_features(
    mprpc_file_transfer_proto
    PUBLIC
    cxx_std_17
)

add_library(mprpc_file_transfer_server STATIC
    server/upload_session_manager.cc
    server/file_transfer_service.cc
)

target_include_directories(
    mprpc_file_transfer_server
    PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/server
)

target_link_libraries(
    mprpc_file_transfer_server
    PUBLIC
    mprpc_file_transfer_proto
    PRIVATE
    OpenSSL::Crypto
    ZLIB::ZLIB
    muduo_base
    Threads::Threads
    mprpc_executor
)

target_compile_features(
    mprpc_file_transfer_server
    PUBLIC
    cxx_std_17
)

add_library(mprpc_file_transfer_client STATIC
    client/sequential_file_uploader.cc
    client/parallel_file_uploader.cc
)

target_include_directories(
    mprpc_file_transfer_client
    PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/client
)

target_link_libraries(
    mprpc_file_transfer_client
    PUBLIC
    mprpc_file_transfer_proto
    mprpc
    PRIVATE
    OpenSSL::Crypto
    ZLIB::ZLIB
)

target_compile_features(
    mprpc_file_transfer_client
    PUBLIC
    cxx_std_17
)

add_executable(file_transfer_server
    app/file_transfer_server_main.cc
)

target_link_libraries(
    file_transfer_server
    PRIVATE
    mprpc_file_transfer_server
    mprpc
)

add_executable(file_transfer_client
    app/file_transfer_client_main.cc
)

target_link_libraries(
    file_transfer_client
    PRIVATE
    mprpc_file_transfer_client
)

add_executable(file_transfer_manager_test
    test/upload_session_manager_test.cc
)

target_link_libraries(
    file_transfer_manager_test
    PRIVATE
    mprpc_file_transfer_server
)

add_executable(file_transfer_service_test
    test/file_transfer_service_test.cc
)

target_link_libraries(
    file_transfer_service_test
    PRIVATE
    mprpc_file_transfer_server
)

add_executable(file_transfer_overload_test
    test/file_transfer_overload_test.cc
)

target_link_libraries(
    file_transfer_overload_test
    PRIVATE
    mprpc_file_transfer_server
)

if(BUILD_TESTING)
    add_test(NAME file_transfer_manager_test
             COMMAND file_transfer_manager_test)
    add_test(NAME file_transfer_service_test
             COMMAND file_transfer_service_test)
    add_test(NAME file_transfer_overload_test
             COMMAND file_transfer_overload_test)
endif()

add_executable(file_transfer_benchmark
    benchmark/file_transfer_benchmark.cc
)

target_link_libraries(
    file_transfer_benchmark
    PRIVATE
    mprpc_file_transfer_client
    Threads::Threads
)

if(BUILD_TESTING)
    add_test(
        NAME file_transfer_benchmark_invalid_profile
        COMMAND file_transfer_benchmark --profile unknown
    )
    set_tests_properties(
        file_transfer_benchmark_invalid_profile
        PROPERTIES WILL_FAIL TRUE
    )

    add_test(
        NAME file_transfer_benchmark_invalid_window
        COMMAND file_transfer_benchmark --profile custom --window 0
    )
    set_tests_properties(
        file_transfer_benchmark_invalid_window
        PROPERTIES WILL_FAIL TRUE
    )

    add_test(
        NAME file_transfer_benchmark_window_overflow
        COMMAND file_transfer_benchmark --profile custom
                --window 4294967297
    )
    set_tests_properties(
        file_transfer_benchmark_window_overflow
        PROPERTIES WILL_FAIL TRUE
    )
endif()
