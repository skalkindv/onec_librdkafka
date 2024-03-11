cmake_minimum_required(VERSION 3.10)
project(rdkafka_onec)

set(CMAKE_CXX_STANDARD 17)
set(TARGET rdkafka_onec)

if( CMAKE_SIZEOF_VOID_P EQUAL 8 )
	set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/../out64)
	set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/../out64)
	set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/../out64)
else()
	set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/../out32)
	set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/../out32)
	set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/../out32)
endif()

list(APPEND SOURCES
    src/exports.def
    src/dllmain.cpp
    src/stdafx.cpp
    src/stdafx.h
    src/export.h
    src/export.cpp
    src/rdkafka.h
    src/rdkafkacpp.h
    src/base64.h
    src/base64.cpp
    src/gettime.h
    src/gettime.cpp
    src/nlohmann/json.hpp
    src/component_types.h
    src/ComponentBaseImp.h
    src/ComponentBaseImp.cpp
    src/data_conversion.h
    src/data_builder.cpp
    src/data_conteiner.cpp
    src/errors.cpp
    src/consumer1c.h
    src/consumer1c.cpp
    src/consumer1c_rebalance_cb.cpp
    src/consumer1c_event_cb.cpp
    src/consumer1c_core.cpp
    src/producer1c.h
    src/producer1c.cpp
    src/producer1c_delivery_report.cpp
    src/producer1c_core.cpp
    src/admin_client1c.h
    src/admin_client1c.cpp
    src/admin_client1c_core.cpp
    )

add_library(${TARGET} SHARED ${SOURCES})
target_include_directories(${TARGET} PRIVATE include)

#----------------------------------------------------------------------------------------------------------------#
if (WIN32 AND NOT MSVC)
    message(FATAL_ERROR "Must be compiled with MSVC on Windows")
endif ()

if (UNIX)
    find_package( Threads )
else()
    add_definitions(-DUNICODE -DWIN32 -D_USRDLL -D_CRT_SECURE_NO_WARNINGS -D_CRT_SECURE_NO_DEPRECATE -DADDINCPP_EXPORTS -D_SILENCE_CXX17_CODECVT_HEADER_DEPRECATION_WARNING)
endif()

if (UNIX)
    if (${CMAKE_SIZEOF_VOID_P} MATCHES "8")
	set(CMAKE_C_FLAGS "-m64 ${CMAKE_C_FLAGS}")
        set(CMAKE_CXX_FLAGS "-m64 ${CMAKE_CXX_FLAGS}")
        target_link_libraries(${TARGET} ${CMAKE_SOURCE_DIR}/lib/linux64/librdkafka++.a)
        target_link_libraries(${TARGET} ${CMAKE_SOURCE_DIR}/lib/linux64/librdkafka-static.a)
    else()
	set(CMAKE_C_FLAGS "-m32 ${CMAKE_C_FLAGS}")
        set(CMAKE_CXX_FLAGS "-m32 ${CMAKE_CXX_FLAGS}")
        target_link_libraries(${TARGET} ${CMAKE_SOURCE_DIR}/lib/linux32/librdkafka++.a)
        target_link_libraries(${TARGET} ${CMAKE_SOURCE_DIR}/lib/linux32/librdkafka-static.a)
    endif()
    target_link_libraries(${TARGET} ${CMAKE_THREAD_LIBS_INIT})
    target_link_libraries(${TARGET} ${CMAKE_DL_LIBS})
endif()

if (WIN32)
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} /MT")
    target_precompile_headers(${TARGET} PUBLIC src/stdafx.h)
	target_link_libraries(${TARGET} "Wldap32.lib")
    if (${CMAKE_SIZEOF_VOID_P} MATCHES "8")
        target_link_libraries(${TARGET} ${CMAKE_SOURCE_DIR}/lib/win64/librdkafka.lib)
        target_link_libraries(${TARGET} ${CMAKE_SOURCE_DIR}/lib/win64/librdkafkacpp.lib)     
        target_link_libraries(${TARGET} ${CMAKE_SOURCE_DIR}/lib/win64/libcurl.lib)
        target_link_libraries(${TARGET} ${CMAKE_SOURCE_DIR}/lib/win64/zlibstat.lib)
        target_link_libraries(${TARGET} ${CMAKE_SOURCE_DIR}/lib/win64/libzstd_static.lib)
        target_link_libraries(${TARGET} ${CMAKE_SOURCE_DIR}/lib/win64/libssl.lib)
        target_link_libraries(${TARGET} ${CMAKE_SOURCE_DIR}/lib/win64/libcrypto.lib)
    else()
	target_link_libraries(${TARGET} ${CMAKE_SOURCE_DIR}/lib/win32/librdkafka.lib)
        target_link_libraries(${TARGET} ${CMAKE_SOURCE_DIR}/lib/win32/librdkafkacpp.lib)        
        target_link_libraries(${TARGET} ${CMAKE_SOURCE_DIR}/lib/win32/libcurl.lib)
        target_link_libraries(${TARGET} ${CMAKE_SOURCE_DIR}/lib/win32/zlibstat.lib)
        target_link_libraries(${TARGET} ${CMAKE_SOURCE_DIR}/lib/win32/libzstd_static.lib)
        target_link_libraries(${TARGET} ${CMAKE_SOURCE_DIR}/lib/win32/libssl.lib)
        target_link_libraries(${TARGET} ${CMAKE_SOURCE_DIR}/lib/win32/libcrypto.lib)
    endif()
endif()

set_target_properties( ${TARGET} PROPERTIES
       POSITION_INDEPENDENT_CODE ON
       CXX_STANDARD 17
       CXX_STANDARD_REQUIRED ON
   )
