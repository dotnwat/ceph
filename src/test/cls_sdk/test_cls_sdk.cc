#include <iostream>
#include <errno.h>

#include "include/rados/librados.hpp"
#include "include/encoding.h"
#include "test/librados/test.h"
#include "gtest/gtest.h"

using namespace librados;

TEST(ClsSDK, TestSDKCoverage) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  ASSERT_EQ(0, ioctx.create("obj", false));

  bufferlist in, out;
  ASSERT_EQ(0, ioctx.exec("obj", "sdk", "test_coverage", in, out));

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}
