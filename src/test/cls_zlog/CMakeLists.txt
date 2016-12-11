add_executable(ceph_test_cls_zlog
    test_cls_zlog.cc
    test_cls_zlog_v2.cc)
set_target_properties(ceph_test_cls_zlog
    PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_cls_zlog
    librados global cls_zlog_client
    ${EXTRALIBS} ${CMAKE_DL_LIBS}
    radostest ${UNITTEST_LIBS})
install(TARGETS ceph_test_cls_zlog
    DESTINATION ${CMAKE_INSTALL_BINDIR})
