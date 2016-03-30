#include <iostream>
#include <errno.h>
#include <set>
#include <sstream>
#include <string>

#include "cls/zlog_bench/cls_zlog_bench_client.h"
#include "gtest/gtest.h"
#include "include/rados/librados.hpp"
#include "test/librados/test.h"

TEST(ClsZlogBench, Append) {
  librados::Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  librados::IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  int ret = ioctx.create("oid", true);
  ASSERT_EQ(ret, 0);

  uint64_t size;
  ret = ioctx.stat("oid", &size, NULL);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(size, 0ULL);

  char buf[1024];
  ceph::bufferlist data;
  data.append(buf, sizeof(buf));

  // append #1
  librados::ObjectWriteOperation op;
  zlog_bench::cls_zlog_bench_append(op, 123, 456, data);
  ret = ioctx.operate("oid", &op);
  ASSERT_EQ(ret, 0);

  ret = ioctx.stat("oid", &size, NULL);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(size, data.length());

  // append #2
  data.clear();
  data.append(buf, sizeof(buf));
  zlog_bench::cls_zlog_bench_append(op, 123, 457, data);
  ret = ioctx.operate("oid", &op);
  ASSERT_EQ(ret, 0);

  ret = ioctx.stat("oid", &size, NULL);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(size, 2*data.length());

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}
