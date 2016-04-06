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

TEST(ClsZlogBench, AppendSimHdrIdx) {
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
  zlog_bench::cls_zlog_bench_append_sim_hdr_idx(*op, 123, 456, data);
  ret = ioctx.operate("oid", op);
  ASSERT_EQ(ret, 0);

  ret = ioctx.stat("oid", &size, NULL);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(size, 4096+data.length());

  // append #2
  for (int i = 0; i < 10; i++) {
    data.clear();
    data.append(buf, sizeof(buf));
    op = new librados::ObjectWriteOperation;
    zlog_bench::cls_zlog_bench_append_sim_hdr_idx(*op, 123, i*457, data);
    ret = ioctx.operate("oid", op);
    ASSERT_EQ(ret, 0);
  }

  ret = ioctx.stat("oid", &size, NULL);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(size, 4096 + 11*data.length());

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(ClsZlogBench, AppendWROnly) {
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
  zlog_bench::cls_zlog_bench_append_wronly(*op, 123, 456, data);
  ret = ioctx.operate("oid", op);
  ASSERT_EQ(ret, 0);

  ret = ioctx.stat("oid", &size, NULL);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(size, data.length());

  // append #2
  data.clear();
  data.append(buf, sizeof(buf));
  op = new librados::ObjectWriteOperation;
  zlog_bench::cls_zlog_bench_append_wronly(*op, 123, 457, data);
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
  ASSERT_EQ(size, (unsigned)0);

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
  ASSERT_EQ(size, (unsigned)0);

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

  kvs.clear();
  ret = ioctx.omap_get_vals("oid", "", 100, &kvs);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(kvs.size(), (unsigned)2);

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

  kvs.clear();
  ret = ioctx.omap_get_vals("oid", "", 100, &kvs);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(kvs.size(), (unsigned)3);

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

  kvs.clear();
  ret = ioctx.omap_get_vals("oid", "", 100, &kvs);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(kvs.size(), (unsigned)3);

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(ClsZlogBench, MapWriteNull) {
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

  // init will set the epoch in the omap
  std::map<std::string, ceph::bufferlist> kvs;
  ret = ioctx.omap_get_vals("oid", "", 100, &kvs);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(kvs.size(), (unsigned)0);

  char buf[1024];
  ceph::bufferlist data;
  data.append(buf, sizeof(buf));

  // append #1
  librados::ObjectWriteOperation *op = new librados::ObjectWriteOperation;
  zlog_bench::cls_zlog_bench_map_write_null(*op, 9, 456, data);
  ret = ioctx.operate("oid", op);
  ASSERT_EQ(ret, 0);

  ret = ioctx.stat("oid", &size, NULL);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(size, 0ULL);

  kvs.clear();
  ret = ioctx.omap_get_vals("oid", "", 100, &kvs);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(kvs.size(), (unsigned)1);
  ASSERT_TRUE(kvs.find("____zlog.pos.00000000000000000456") != kvs.end());
  ASSERT_EQ(kvs.find("____zlog.pos.00000000000000000456")->second.length(), data.length());

  // append #1
  data.clear();
  data.append(buf, sizeof(buf));
  op = new librados::ObjectWriteOperation;
  zlog_bench::cls_zlog_bench_map_write_null(*op, 9, 457, data);
  ret = ioctx.operate("oid", op);
  ASSERT_EQ(ret, 0);

  ret = ioctx.stat("oid", &size, NULL);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(size, 0ULL);

  kvs.clear();
  ret = ioctx.omap_get_vals("oid", "", 100, &kvs);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(kvs.size(), (unsigned)2);
  ASSERT_TRUE(kvs.find("____zlog.pos.00000000000000000456") != kvs.end());
  ASSERT_TRUE(kvs.find("____zlog.pos.00000000000000000457") != kvs.end());
  ASSERT_EQ(kvs.find("____zlog.pos.00000000000000000456")->second.length(), data.length());
  ASSERT_EQ(kvs.find("____zlog.pos.00000000000000000457")->second.length(), data.length());

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(ClsZlogBench, MapWriteNullWROnly) {
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

  // init will set the epoch in the omap
  std::map<std::string, ceph::bufferlist> kvs;
  ret = ioctx.omap_get_vals("oid", "", 100, &kvs);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(kvs.size(), (unsigned)0);

  char buf[1024];
  ceph::bufferlist data;
  data.append(buf, sizeof(buf));

  // append #1
  librados::ObjectWriteOperation *op = new librados::ObjectWriteOperation;
  zlog_bench::cls_zlog_bench_map_write_null_wronly(*op, 9, 456, data);
  ret = ioctx.operate("oid", op);
  ASSERT_EQ(ret, 0);

  ret = ioctx.stat("oid", &size, NULL);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(size, 0ULL);

  kvs.clear();
  ret = ioctx.omap_get_vals("oid", "", 100, &kvs);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(kvs.size(), (unsigned)1);
  ASSERT_TRUE(kvs.find("____zlog.pos.00000000000000000456") != kvs.end());
  ASSERT_EQ(kvs.find("____zlog.pos.00000000000000000456")->second.length(), data.length());

  // append #1
  data.clear();
  data.append(buf, sizeof(buf));
  op = new librados::ObjectWriteOperation;
  zlog_bench::cls_zlog_bench_map_write_null_wronly(*op, 9, 457, data);
  ret = ioctx.operate("oid", op);
  ASSERT_EQ(ret, 0);

  ret = ioctx.stat("oid", &size, NULL);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(size, 0ULL);

  kvs.clear();
  ret = ioctx.omap_get_vals("oid", "", 100, &kvs);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(kvs.size(), (unsigned)2);
  ASSERT_TRUE(kvs.find("____zlog.pos.00000000000000000456") != kvs.end());
  ASSERT_TRUE(kvs.find("____zlog.pos.00000000000000000457") != kvs.end());
  ASSERT_EQ(kvs.find("____zlog.pos.00000000000000000456")->second.length(), data.length());
  ASSERT_EQ(kvs.find("____zlog.pos.00000000000000000457")->second.length(), data.length());

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(ClsZlogBench, MapWriteFull) {
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

  // init will set the epoch in the omap
  std::map<std::string, ceph::bufferlist> kvs;
  ret = ioctx.omap_get_vals("oid", "", 100, &kvs);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(kvs.size(), (unsigned)0);

  char buf[1024];
  ceph::bufferlist data;
  data.append(buf, sizeof(buf));

  // append failure because of epoch not being set
  librados::ObjectWriteOperation *op = new librados::ObjectWriteOperation;
  zlog_bench::cls_zlog_bench_map_write_full(*op, 9, 456, data);
  ret = ioctx.operate("oid", op);
  ASSERT_EQ(ret, -ENOENT);

  // omap should be empty
  kvs.clear();
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
  zlog_bench::cls_zlog_bench_map_write_full(*op, 9, 456, data);
  ret = ioctx.operate("oid", op);
  ASSERT_EQ(ret, -EINVAL);

  // object size is still 0
  ret = ioctx.stat("oid", &size, NULL);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(size, (unsigned)0);

  kvs.clear();
  ret = ioctx.omap_get_vals("oid", "", 100, &kvs);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(kvs.size(), (unsigned)1);
  ASSERT_TRUE(kvs.find("____zlog.epoch") != kvs.end());

  // append #1
  data.clear();
  data.append(buf, sizeof(buf));
  op = new librados::ObjectWriteOperation;
  zlog_bench::cls_zlog_bench_map_write_full(*op, 123, 456, data);
  ret = ioctx.operate("oid", op);
  ASSERT_EQ(ret, 0);

  ret = ioctx.stat("oid", &size, NULL);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(size, (unsigned)0);

  kvs.clear();
  ret = ioctx.omap_get_vals("oid", "", 100, &kvs);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(kvs.size(), (unsigned)2);
  ASSERT_TRUE(kvs.find("____zlog.epoch") != kvs.end());
  ASSERT_TRUE(kvs.find("____zlog.pos.00000000000000000456") != kvs.end());
  ASSERT_GE(kvs.find("____zlog.pos.00000000000000000456")->second.length(), data.length());

  // append #2
  data.clear();
  data.append(buf, sizeof(buf));
  op = new librados::ObjectWriteOperation;
  zlog_bench::cls_zlog_bench_map_write_full(*op, 123, 457, data);
  ret = ioctx.operate("oid", op);
  ASSERT_EQ(ret, 0);

  ret = ioctx.stat("oid", &size, NULL);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(size, (unsigned)0);

  kvs.clear();
  ret = ioctx.omap_get_vals("oid", "", 100, &kvs);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(kvs.size(), (unsigned)3);
  ASSERT_TRUE(kvs.find("____zlog.epoch") != kvs.end());
  ASSERT_TRUE(kvs.find("____zlog.pos.00000000000000000456") != kvs.end());
  ASSERT_TRUE(kvs.find("____zlog.pos.00000000000000000457") != kvs.end());
  ASSERT_GE(kvs.find("____zlog.pos.00000000000000000456")->second.length(), data.length());
  ASSERT_GE(kvs.find("____zlog.pos.00000000000000000457")->second.length(), data.length());

  // append #3 fails because write-once
  data.clear();
  data.append(buf, sizeof(buf));
  op = new librados::ObjectWriteOperation;
  zlog_bench::cls_zlog_bench_map_write_full(*op, 123, 457, data);
  ret = ioctx.operate("oid", op);
  ASSERT_EQ(ret, -EROFS);

  ret = ioctx.stat("oid", &size, NULL);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(size, (unsigned)0);

  kvs.clear();
  ret = ioctx.omap_get_vals("oid", "", 100, &kvs);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(kvs.size(), (unsigned)3);
  ASSERT_TRUE(kvs.find("____zlog.epoch") != kvs.end());
  ASSERT_TRUE(kvs.find("____zlog.pos.00000000000000000456") != kvs.end());
  ASSERT_TRUE(kvs.find("____zlog.pos.00000000000000000457") != kvs.end());
  ASSERT_GE(kvs.find("____zlog.pos.00000000000000000456")->second.length(), data.length());
  ASSERT_GE(kvs.find("____zlog.pos.00000000000000000457")->second.length(), data.length());

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(ClsZlogBench, StreamWriteNull) {
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
  zlog_bench::cls_zlog_bench_stream_write_null(*op, 123, 0, data);
  ret = ioctx.operate("oid", op);
  ASSERT_EQ(ret, 0);

  ret = ioctx.stat("oid", &size, NULL);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(size, data.length());

  // append #2
  data.clear();
  data.append(buf, sizeof(buf));
  op = new librados::ObjectWriteOperation;
  zlog_bench::cls_zlog_bench_stream_write_null(*op, 123, 2*data.length(), data);
  ret = ioctx.operate("oid", op);
  ASSERT_EQ(ret, 0);

  ret = ioctx.stat("oid", &size, NULL);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(size, 3*data.length());

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(ClsZlogBench, StreamWriteNullWROnly) {
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
  zlog_bench::cls_zlog_bench_stream_write_null_wronly(*op, 123, 0, data);
  ret = ioctx.operate("oid", op);
  ASSERT_EQ(ret, 0);

  ret = ioctx.stat("oid", &size, NULL);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(size, data.length());

  // append #2
  data.clear();
  data.append(buf, sizeof(buf));
  op = new librados::ObjectWriteOperation;
  zlog_bench::cls_zlog_bench_stream_write_null_wronly(*op, 123, 2*data.length(), data);
  ret = ioctx.operate("oid", op);
  ASSERT_EQ(ret, 0);

  ret = ioctx.stat("oid", &size, NULL);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(size, 3*data.length());

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(ClsZlogBench, StreamWriteFull) {
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
  zlog_bench::cls_zlog_bench_stream_write_full(*op, 9, 456, data);
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
  zlog_bench::cls_zlog_bench_stream_write_full(*op, 9, 456, data);
  ret = ioctx.operate("oid", op);
  ASSERT_EQ(ret, -EINVAL);

  // object size is still 0
  ret = ioctx.stat("oid", &size, NULL);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(size, (unsigned)0);

  // append #1
  data.clear();
  data.append(buf, sizeof(buf));
  op = new librados::ObjectWriteOperation;
  zlog_bench::cls_zlog_bench_stream_write_full(*op, 123, 0, data);
  ret = ioctx.operate("oid", op);
  ASSERT_EQ(ret, 0);

  ret = ioctx.stat("oid", &size, NULL);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(size, data.length());

  kvs.clear();
  ret = ioctx.omap_get_vals("oid", "", 100, &kvs);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(kvs.size(), (unsigned)2);
  ASSERT_TRUE(kvs.find("____zlog.epoch") != kvs.end());
  ASSERT_TRUE(kvs.find("____zlog.pos.00000000000000000000") != kvs.end());

  // append #2
  data.clear();
  data.append(buf, sizeof(buf));
  op = new librados::ObjectWriteOperation;
  zlog_bench::cls_zlog_bench_stream_write_full(*op, 123, 2*data.length(), data);
  ret = ioctx.operate("oid", op);
  ASSERT_EQ(ret, 0);

  ret = ioctx.stat("oid", &size, NULL);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(size, 3*data.length());

  kvs.clear();
  ret = ioctx.omap_get_vals("oid", "", 100, &kvs);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(kvs.size(), (unsigned)3);
  ASSERT_TRUE(kvs.find("____zlog.epoch") != kvs.end());
  ASSERT_TRUE(kvs.find("____zlog.pos.00000000000000000000") != kvs.end());
  ASSERT_TRUE(kvs.find("____zlog.pos.00000000000000002048") != kvs.end());

  // append #3 fails because write-once
  data.clear();
  data.append(buf, sizeof(buf));
  op = new librados::ObjectWriteOperation;
  zlog_bench::cls_zlog_bench_stream_write_full(*op, 123, 0, data);
  ret = ioctx.operate("oid", op);
  ASSERT_EQ(ret, -EROFS);

  data.clear();
  data.append(buf, sizeof(buf));
  op = new librados::ObjectWriteOperation;
  zlog_bench::cls_zlog_bench_stream_write_full(*op, 123, 2*data.length(), data);
  ret = ioctx.operate("oid", op);
  ASSERT_EQ(ret, -EROFS);

  ret = ioctx.stat("oid", &size, NULL);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(size, 3*data.length());

  data.clear();
  data.append(buf, sizeof(buf));
  op = new librados::ObjectWriteOperation;
  zlog_bench::cls_zlog_bench_stream_write_full(*op, 123, data.length(), data);
  ret = ioctx.operate("oid", op);
  ASSERT_EQ(ret, 0);

  data.clear();
  data.append(buf, sizeof(buf));
  op = new librados::ObjectWriteOperation;
  zlog_bench::cls_zlog_bench_stream_write_full(*op, 123, data.length(), data);
  ret = ioctx.operate("oid", op);
  ASSERT_EQ(ret, -EROFS);

  ret = ioctx.stat("oid", &size, NULL);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(size, 3*data.length());

  kvs.clear();
  ret = ioctx.omap_get_vals("oid", "", 100, &kvs);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(kvs.size(), (unsigned)4);
  ASSERT_TRUE(kvs.find("____zlog.epoch") != kvs.end());
  ASSERT_TRUE(kvs.find("____zlog.pos.00000000000000000000") != kvs.end());
  ASSERT_TRUE(kvs.find("____zlog.pos.00000000000000001024") != kvs.end());
  ASSERT_TRUE(kvs.find("____zlog.pos.00000000000000002048") != kvs.end());

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}
