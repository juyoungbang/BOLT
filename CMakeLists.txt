cmake_minimum_required(VERSION 3.16)

project(
  BOLT
  DESCRIPTION "Disaggregated environment based ordered index"
  LANGUAGES CXX
)

find_package(Boost REQUIRED)
find_package(Threads REQUIRED)

set(CMAKE_C_FLAGS "-O3 -g")

if(STATS)
  add_definitions(-DSTATS)
endif()

add_definitions(-DBOOST_COROUTINES_NO_DEPRECATION_WARNING)

set(CMAKE_CXX_FLAGS "${CMAKE_C_FLAGS} -std=c++17")
set(BOLT_SRC ${PROJECT_SOURCE_DIR}/src)

set(INCLUDE_BASE ${PROJECT_SOURCE_DIR}/include)
include_directories(${INCLUDE_BASE})

# Make bolt library
file(GLOB_RECURSE BOLT_FILE ${BOLT_SRC}/*.cpp)
add_library(bolt STATIC ${BOLT_FILE})
target_link_libraries(bolt numa cityhash tbb ${Boost_LIBRARIES} boost_fiber boost_coroutine boost_context)

# Make executable benchmarks
file(GLOB BENCH_SRC ${PROJECT_SOURCE_DIR}/test/*.cpp)
foreach (BENCH ${BENCH_SRC})
  get_filename_component(BENCH_NAME ${BENCH} NAME_WE)
  add_executable(${BENCH_NAME} ${BENCH})
  target_link_libraries(${BENCH_NAME} bolt pthread ibverbs)
endforeach()