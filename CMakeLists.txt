cmake_minimum_required(VERSION 3.20)
project(nexstream_publisher VERSION 0.3.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(NEXSTREAM_INGEST_BINARY "" CACHE FILEPATH "Sprint 02 nexstream-ingest binary for integration tests")
find_package(nexstream_contracts 0.1 REQUIRED CONFIG)
find_package(OpenSSL REQUIRED)
find_package(Threads REQUIRED)
add_library(nexstream_publisher_core
    src/application/ack_interpreter.cpp
    src/infrastructure/frame_codec.cpp
    src/infrastructure/nonblocking_stream.cpp
    src/infrastructure/tcp_publisher_client.cpp)
add_library(nexstream::publisher_core ALIAS nexstream_publisher_core)
target_link_libraries(nexstream_publisher_core PUBLIC nexstream::contracts_proto OpenSSL::SSL
    OpenSSL::Crypto Threads::Threads)
target_include_directories(nexstream_publisher_core PUBLIC include)
target_compile_options(nexstream_publisher_core PRIVATE -Wall -Wextra -Wpedantic -Wconversion)

add_executable(nexstream-publisher src/bootstrap/main.cpp)
target_link_libraries(nexstream-publisher PRIVATE nexstream_publisher_core)
target_compile_options(nexstream-publisher PRIVATE -Wall -Wextra -Wpedantic)

include(CTest)
if(BUILD_TESTING)
    if(NOT EXISTS "${NEXSTREAM_INGEST_BINARY}")
        message(FATAL_ERROR "Set -DNEXSTREAM_INGEST_BINARY to the built Sprint 02 gateway")
    endif()
    add_executable(nexstream_publisher_ack_tests tests/unit/ack_interpreter_tests.cpp)
    target_link_libraries(nexstream_publisher_ack_tests PRIVATE nexstream_publisher_core)
    target_include_directories(nexstream_publisher_ack_tests PRIVATE tests)
    add_test(NAME ack_interpreter_unit COMMAND nexstream_publisher_ack_tests)
    set_tests_properties(ack_interpreter_unit PROPERTIES LABELS unit)

    add_executable(nexstream_publisher_validation_tests tests/unit/publisher_validation_tests.cpp)
    target_link_libraries(nexstream_publisher_validation_tests PRIVATE nexstream_publisher_core)
    target_include_directories(nexstream_publisher_validation_tests PRIVATE tests)
    add_test(NAME publisher_validation_unit COMMAND nexstream_publisher_validation_tests)
    set_tests_properties(publisher_validation_unit PROPERTIES LABELS unit)

    add_executable(nexstream_publisher_io_tests tests/unit/nonblocking_io_tests.cpp)
    target_link_libraries(nexstream_publisher_io_tests PRIVATE nexstream_publisher_core pthread)
    target_include_directories(nexstream_publisher_io_tests PRIVATE tests)
    add_test(NAME nonblocking_io_unit COMMAND nexstream_publisher_io_tests)
    set_tests_properties(nonblocking_io_unit PROPERTIES LABELS unit TIMEOUT 10)

    add_executable(nexstream_publisher_gateway_tests tests/integration/gateway_integration_tests.cpp)
    target_link_libraries(nexstream_publisher_gateway_tests PRIVATE nexstream_publisher_core pthread)
    target_include_directories(nexstream_publisher_gateway_tests PRIVATE tests)
    target_compile_definitions(nexstream_publisher_gateway_tests PRIVATE
        NEXSTREAM_INGEST_BINARY="${NEXSTREAM_INGEST_BINARY}")
    add_test(NAME gateway_sprint02_integration COMMAND nexstream_publisher_gateway_tests)
    set_tests_properties(gateway_sprint02_integration PROPERTIES LABELS integration TIMEOUT 20)
endif()
