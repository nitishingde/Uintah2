cmake_minimum_required(VERSION 3.17)
project(Uintah2)

find_package(MPI)

set(CMAKE_CXX_STANDARD 20)

include(FetchContent)

FetchContent_Declare(
        sigslot
        GIT_REPOSITORY https://github.com/palacaze/sigslot
        GIT_TAG        19a6f0f5ea11fc121fe67f81fd5e491f2d7a4637 # v1.2.0
)
FetchContent_MakeAvailable(sigslot)

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
    PUBLIC
        Pal::Sigslot
)

add_executable(comm_test main.cpp)
target_link_libraries(comm_test
    PUBLIC
        comm
)
