cmake_minimum_required(VERSION 3.12)
project(lsm_kv_store CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

file(GLOB_RECURSE SRC_FILES "src/*.cpp")
list(REMOVE_ITEM SRC_FILES "${CMAKE_SOURCE_DIR}/src/main.cpp")

add_library(lsmkv_all ${SRC_FILES})
target_include_directories(lsmkv_all PUBLIC include src)

add_executable(lsmkv_main src/main.cpp)
target_link_libraries(lsmkv_main lsmkv_all)

enable_testing()
add_executable(test_skiplist test/test_skiplist.cpp)
target_link_libraries(test_skiplist lsmkv_all)
add_test(NAME skiplist COMMAND test_skiplist)

add_executable(test_sstable test/test_sstable.cpp)
target_link_libraries(test_sstable lsmkv_all)
add_test(NAME sstable COMMAND test_sstable)
