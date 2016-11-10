add_executable(copyfrom copyfrom.cc)
target_link_libraries(copyfrom librados global ${Boost_PROGRAM_OPTIONS_LIBRARY})
install(TARGETS copyfrom DESTINATION bin)
