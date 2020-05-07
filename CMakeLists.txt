cmake_minimum_required(VERSION 3.11...3.13)
if(${CMAKE_VERSION} VERSION_LESS 3.12)
    cmake_policy(VERSION ${CMAKE_MAJOR_VERSION}.${CMAKE_MINOR_VERSION})
endif()

include(FetchContent)

if(${CMAKE_VERSION} VERSION_LESS 3.14)
    macro(FetchContent_MakeAvailable NAME)
        FetchContent_GetProperties(${NAME})
        if(NOT ${NAME}_POPULATED)
            FetchContent_Populate(${NAME})
            #add_subdirectory(${${NAME}_SOURCE_DIR} ${${NAME}_BINARY_DIR})
        endif()
    endmacro()
endif()

FetchContent_Declare(
    zstd
    GIT_REPOSITORY  https://github.com/facebook/zstd.git
    GIT_TAG         v1.4.4
)
FetchContent_MakeAvailable(zstd)
include_directories(${zstd_SOURCE_DIR}/lib/common ${zstd_SOURCE_DIR}/contrib/seekable_format)

set(CMAKE_C_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
add_definitions(-DXXH_NAMESPACE=ZSTD_)

project(sqlite_zstd_rovfs VERSION 1.0
        DESCRIPTION "SQLite read-only VFS with zstd compression"
        LANGUAGES C)

add_executable(zstd_seekable_compress
                ${zstd_SOURCE_DIR}/lib/common/xxhash.c
                ${zstd_SOURCE_DIR}/lib/common/pool.c
                ${zstd_SOURCE_DIR}/lib/common/error_private.c
                ${zstd_SOURCE_DIR}/contrib/seekable_format/examples/parallel_compression.c 
                ${zstd_SOURCE_DIR}/contrib/seekable_format/zstdseek_compress.c)
target_link_libraries(zstd_seekable_compress PRIVATE zstd)

add_library(zstd_rovfs SHARED
                zstd_rovfs.c
                ${zstd_SOURCE_DIR}/lib/common/xxhash.c
                ${zstd_SOURCE_DIR}/lib/common/error_private.c
                ${zstd_SOURCE_DIR}/contrib/seekable_format/zstdseek_decompress.c)
target_link_libraries(zstd_rovfs PRIVATE zstd)
SET_TARGET_PROPERTIES(zstd_rovfs PROPERTIES PREFIX "")

add_custom_target(pretty COMMAND clang-format -i -style='{IndentWidth: 4, ColumnLimit: 100}' ${CMAKE_CURRENT_SOURCE_DIR}/zstd_rovfs.c)

include(CTest)
enable_testing()
add_test(NAME pytest COMMAND pytest ${CMAKE_CURRENT_SOURCE_DIR})
