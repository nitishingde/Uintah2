cmake_minimum_required(VERSION 3.17)
project(Uintah2)

find_package(MPI)

set(CMAKE_CXX_STANDARD 20)

add_library(comm
        src/comm.cpp
)
add_library(Uintah2::comm ALIAS comm)
target_include_directories(comm
    PUBLIC
        $<INSTALL_INTERFACE:include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
)
target_link_libraries(comm
    PRIVATE
        MPI::MPI_CXX
)

add_executable(comm_test main.cpp)
target_link_libraries(comm_test
    PUBLIC
        comm
)

add_executable(comm_benchmark benchmark.cpp)
target_link_libraries(comm_benchmark
    PUBLIC
        comm
)
