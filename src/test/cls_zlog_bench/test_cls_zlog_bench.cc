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
  librados::ObjectWriteOperation *op = new librados::ObjectWriteOperation;
  zlog_bench::cls_zlog_bench_append(*op, 123, 456, data);
  ret = ioctx.operate("oid", op);
  ASSERT_EQ(ret, 0);

  ret = ioctx.stat("oid", &size, NULL);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(size, data.length());

  // append #2
  data.clear();
  data.append(buf, sizeof(buf));
  op = new librados::ObjectWriteOperation;
  zlog_bench::cls_zlog_bench_append(*op, 123, 457, data);
  ret = ioctx.operate("oid", op);
  ASSERT_EQ(ret, 0);

  ret = ioctx.stat("oid", &size, NULL);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(size, 2*data.length());

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(ClsZlogBench, AppendCheckEpoch) {
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

  // append failure
  librados::ObjectWriteOperation *op = new librados::ObjectWriteOperation;
  zlog_bench::cls_zlog_bench_append_check_epoch(*op, 9, 456, data);
  ret = ioctx.operate("oid", op);
  ASSERT_EQ(ret, -ENOENT);

  // omap should be empty
  std::map<std::string, ceph::bufferlist> kvs;
  ret = ioctx.omap_get_vals("oid", "", 100, &kvs);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(kvs.size(), (unsigned)0);

  // init stored epoch
  op = new librados::ObjectWriteOperation;
  zlog_bench::cls_zlog_bench_append_init(*op);
  ret = ioctx.operate("oid", op);
  ASSERT_EQ(ret, 0);

  // init will set the epoch in the omap
  ret = ioctx.omap_get_vals("oid", "", 100, &kvs);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(kvs.size(), (unsigned)1);
  ASSERT_TRUE(kvs.find("____zlog.epoch") != kvs.end());

  // append #1 fails because old epoch
  data.clear();
  data.append(buf, sizeof(buf));
  op = new librados::ObjectWriteOperation;
  zlog_bench::cls_zlog_bench_append_check_epoch(*op, 9, 456, data);
  ret = ioctx.operate("oid", op);
  ASSERT_EQ(ret, -EINVAL);

  // object size is still 0
  ret = ioctx.stat("oid", &size, NULL);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(size, 0);

  // append #1
  data.clear();
  data.append(buf, sizeof(buf));
  op = new librados::ObjectWriteOperation;
  zlog_bench::cls_zlog_bench_append_check_epoch(*op, 123, 456, data);
  ret = ioctx.operate("oid", op);
  ASSERT_EQ(ret, 0);

  ret = ioctx.stat("oid", &size, NULL);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(size, data.length());

  // append #2
  data.clear();
  data.append(buf, sizeof(buf));
  op = new librados::ObjectWriteOperation;
  zlog_bench::cls_zlog_bench_append_check_epoch(*op, 123, 457, data);
  ret = ioctx.operate("oid", op);
  ASSERT_EQ(ret, 0);

  ret = ioctx.stat("oid", &size, NULL);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(size, 2*data.length());

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(ClsZlogBench, AppendOmapIndex) {
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

  // append failure because of epoch not being set
  librados::ObjectWriteOperation *op = new librados::ObjectWriteOperation;
  zlog_bench::cls_zlog_bench_append_omap_index(*op, 9, 456, data);
  ret = ioctx.operate("oid", op);
  ASSERT_EQ(ret, -ENOENT);

  // omap should be empty
  std::map<std::string, ceph::bufferlist> kvs;
  ret = ioctx.omap_get_vals("oid", "", 100, &kvs);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(kvs.size(), (unsigned)0);

  // init stored epoch
  op = new librados::ObjectWriteOperation;
  zlog_bench::cls_zlog_bench_append_init(*op);
  ret = ioctx.operate("oid", op);
  ASSERT_EQ(ret, 0);

  // init will set the epoch in the omap
  ret = ioctx.omap_get_vals("oid", "", 100, &kvs);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(kvs.size(), (unsigned)1);
  ASSERT_TRUE(kvs.find("____zlog.epoch") != kvs.end());

  // append #1 fails because old epoch
  data.clear();
  data.append(buf, sizeof(buf));
  op = new librados::ObjectWriteOperation;
  zlog_bench::cls_zlog_bench_append_omap_index(*op, 9, 456, data);
  ret = ioctx.operate("oid", op);
  ASSERT_EQ(ret, -EINVAL);

  // object size is still 0
  ret = ioctx.stat("oid", &size, NULL);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(size, 0);

  // append #1
  data.clear();
  data.append(buf, sizeof(buf));
  op = new librados::ObjectWriteOperation;
  zlog_bench::cls_zlog_bench_append_omap_index(*op, 123, 456, data);
  ret = ioctx.operate("oid", op);
  ASSERT_EQ(ret, 0);

  ret = ioctx.stat("oid", &size, NULL);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(size, data.length());

  // append #2
  data.clear();
  data.append(buf, sizeof(buf));
  op = new librados::ObjectWriteOperation;
  zlog_bench::cls_zlog_bench_append_omap_index(*op, 123, 457, data);
  ret = ioctx.operate("oid", op);
  ASSERT_EQ(ret, 0);

  ret = ioctx.stat("oid", &size, NULL);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(size, 2*data.length());

  // append #3 fails because write-once
  data.clear();
  data.append(buf, sizeof(buf));
  op = new librados::ObjectWriteOperation;
  zlog_bench::cls_zlog_bench_append_omap_index(*op, 123, 457, data);
  ret = ioctx.operate("oid", op);
  ASSERT_EQ(ret, -EROFS);

  ret = ioctx.stat("oid", &size, NULL);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(size, 2*data.length());

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}
