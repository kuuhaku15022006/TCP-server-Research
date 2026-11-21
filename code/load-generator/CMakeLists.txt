add_executable(loadgen main.cpp)

target_include_directories(loadgen PRIVATE
    ${CMAKE_SOURCE_DIR}/code/common
)

target_link_libraries(loadgen PRIVATE Threads::Threads)

if(WIN32)
    target_link_libraries(loadgen PRIVATE ws2_32)
endif()