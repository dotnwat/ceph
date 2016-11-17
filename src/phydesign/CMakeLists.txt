add_executable(phydesign-bench phydesign-bench.cc)
target_link_libraries(phydesign-bench librados global ${CMAKE_DL_LIBS} ${Boost_PROGRAM_OPTIONS_LIBRARY})

add_executable(pd-batch-write batch-write.cc)
target_link_libraries(pd-batch-write librados global ${CMAKE_DL_LIBS} ${Boost_PROGRAM_OPTIONS_LIBRARY})
