cmake_minimum_required(VERSION 3.20)

project(llfcchat_loadtest LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Boost 1.70 REQUIRED)

add_executable(llfcchat_loadtest
    src/main.cpp
)

target_include_directories(llfcchat_loadtest PRIVATE ${Boost_INCLUDE_DIRS})
target_compile_definitions(llfcchat_loadtest PRIVATE
    BOOST_ERROR_CODE_HEADER_ONLY
    BOOST_SYSTEM_NO_DEPRECATED
)

if(MSVC)
    target_compile_options(llfcchat_loadtest PRIVATE /W4 /permissive-)
else()
    target_compile_options(llfcchat_loadtest PRIVATE -Wall -Wextra -Wpedantic)
endif()

if(WIN32)
    target_compile_definitions(llfcchat_loadtest PRIVATE _WIN32_WINNT=0x0601)
    target_link_libraries(llfcchat_loadtest PRIVATE ws2_32 mswsock)
endif()
