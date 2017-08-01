add_executable(ceph_test_cls_zlog
  test_cls_zlog.cc
  $<TARGET_OBJECTS:common_texttable_obj>)
set_target_properties(ceph_test_cls_zlog PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_cls_zlog
  cls_zlog_client
  librados
  global
  ${UNITTEST_LIBS}
  ${CMAKE_DL_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  ${PROTOBUF_LIBRARIES}
  radostest)
target_include_directories(ceph_test_cls_zlog PUBLIC
    ${PROTOBUF_INCLUDE_DIRS} ${CMAKE_CURRENT_BINARY_DIR})
install(TARGETS
  ceph_test_cls_zlog
  DESTINATION ${CMAKE_INSTALL_BINDIR})
