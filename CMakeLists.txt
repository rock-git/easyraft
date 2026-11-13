cmake_minimum_required(VERSION 3.24)

project(easyraft VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(EASYRAFT_SANITIZERS "Build with AddressSanitizer and UndefinedBehaviorSanitizer" OFF)

add_library(easyraft
  src/engine.cpp
  src/file.cpp
  src/node.cpp
)
target_include_directories(easyraft PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_compile_features(easyraft PUBLIC cxx_std_20)
target_compile_options(easyraft PRIVATE -Wall -Wextra -Werror -Wpedantic -Wshadow)

include(CTest)
if(BUILD_TESTING)
  include(FetchContent)
  FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/refs/tags/v1.15.2.zip
    DOWNLOAD_EXTRACT_TIMESTAMP ON
  )
  set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
  set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(googletest)

  add_executable(easyraft_tests
    tests/engine_test.cpp
    tests/election_test.cpp
    tests/replication_test.cpp
    tests/persistence_test.cpp
    tests/file_storage_test.cpp
    tests/partition_test.cpp
    tests/node_test.cpp
    tests/membership_test.cpp
    tests/snapshot_test.cpp
  )
  target_link_libraries(easyraft_tests PRIVATE easyraft GTest::gtest_main)
  target_compile_features(easyraft_tests PRIVATE cxx_std_20)
  target_compile_options(easyraft_tests PRIVATE -Wall -Wextra -Werror -Wpedantic -Wshadow)
  include(GoogleTest)
  gtest_discover_tests(easyraft_tests)
endif()

if(EASYRAFT_SANITIZERS)
  # PUBLIC so a static library still carries the runtime onto every consumer.
  target_compile_options(easyraft PUBLIC -fsanitize=address,undefined -fno-omit-frame-pointer)
  target_link_options(easyraft PUBLIC -fsanitize=address,undefined)
endif()
