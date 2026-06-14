cmake_minimum_required(VERSION 3.20)
project(arrow_data_client CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Point CMake at the Homebrew Arrow install, e.g.:
#   cmake -S . -B build -DCMAKE_PREFIX_PATH=$(brew --prefix apache-arrow)
find_package(Arrow REQUIRED)
find_package(ArrowFlight REQUIRED)
find_package(CURL REQUIRED)

add_executable(arrow_client client.cpp)

# Arrow 14+ ships namespaced imported targets. Prefer shared.
target_link_libraries(arrow_client PRIVATE
    Arrow::arrow_shared
    ArrowFlight::arrow_flight_shared
    CURL::libcurl
)

message(STATUS "Arrow version: ${Arrow_VERSION}")
