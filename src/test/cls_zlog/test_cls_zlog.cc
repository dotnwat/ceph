#include <errno.h>
#include "include/rados/librados.hpp"
#include "test/librados/test.h"
#include "gtest/gtest.h"
#include "cls/zlog/cls_zlog_client.h"

#define ZLOG_EPOCH_KEY "____zlog.epoch"

using namespace librados;

static librados::ObjectWriteOperation *new_op() {
  return new librados::ObjectWriteOperation();
}

static librados::ObjectReadOperation *new_rop() {
  return new librados::ObjectReadOperation();
}

TEST(ClsZlog, Seal) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  // fails to decode input (bad message)
  bufferlist inbl, outbl;
  int ret = ioctx.exec("obj", "zlog", "seal", inbl, outbl);
  ASSERT_EQ(ret, -EINVAL);

  // set the first epoch value on the object to 0
  librados::ObjectWriteOperation *op = new_op();
  zlog::cls_zlog_seal(*op, 0);
  ret = ioctx.operate("obj", op);
  ASSERT_EQ(ret, zlog::CLS_ZLOG_OK);

  // the first epoch can be set to anything (e.g. 100)
  op = new_op();
  zlog::cls_zlog_seal(*op, 100);
  ret = ioctx.operate("obj2", op);
  ASSERT_EQ(ret, zlog::CLS_ZLOG_OK);

  // epochs move strictly forward (99, 100: fail, 101: succeed)
  op = new_op();
  zlog::cls_zlog_seal(*op, 99);
  ret = ioctx.operate("obj2", op);
  ASSERT_EQ(ret, zlog::CLS_ZLOG_INVALID_EPOCH);

  op = new_op();
  zlog::cls_zlog_seal(*op, 100);
  ret = ioctx.operate("obj2", op);
  ASSERT_EQ(ret, zlog::CLS_ZLOG_INVALID_EPOCH);

  op = new_op();
  zlog::cls_zlog_seal(*op, 101);
  ret = ioctx.operate("obj2", op);
  ASSERT_EQ(ret, zlog::CLS_ZLOG_OK);

  // seal will fail if epoch becomes corrupt
  std::map<std::string, bufferlist> vals;
  bufferlist bl;
  bl.append("j");
  vals[ZLOG_EPOCH_KEY] = bl;
  ioctx.omap_set("obj2", vals);
  op = new_op();
  zlog::cls_zlog_seal(*op, 102);
  ret = ioctx.operate("obj2", op);
  ASSERT_EQ(ret, -EIO);

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(ClsZlog, Fill) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  // fails to decode input (bad message)
  bufferlist inbl, outbl;
  int ret = ioctx.exec("obj", "zlog", "fill", inbl, outbl);
  ASSERT_EQ(ret, -EINVAL);

  // rejects fill if no epoch has been set
  librados::ObjectWriteOperation *op = new_op();
  zlog::cls_zlog_fill(*op, 100, 10);
  ret = ioctx.operate("obj", op);
  ASSERT_EQ(ret, -ENOENT);

  // set epoch to 100
  op = new_op();
  zlog::cls_zlog_seal(*op, 100);
  ret = ioctx.operate("obj", op);
  ASSERT_EQ(ret, zlog::CLS_ZLOG_OK);

  // try again
  op = new_op();
  zlog::cls_zlog_fill(*op, 100, 10);
  ret = ioctx.operate("obj", op);
  ASSERT_EQ(ret, zlog::CLS_ZLOG_OK);

  // try with smaller epoch
  op = new_op();
  zlog::cls_zlog_fill(*op, 0, 10);
  ret = ioctx.operate("obj", op);
  ASSERT_EQ(ret, zlog::CLS_ZLOG_STALE_EPOCH);

  // try with larger epoch
  op = new_op();
  zlog::cls_zlog_fill(*op, 1000, 10);
  ret = ioctx.operate("obj", op);
  ASSERT_EQ(ret, zlog::CLS_ZLOG_OK);

  srand(0);

  // fill then fill is OK
  std::set<uint64_t> filled;
  for (int i = 0; i < 100; i++) {
    uint64_t pos = rand();

    filled.insert(pos);
    librados::ObjectWriteOperation op;
    zlog::cls_zlog_fill(op, 100, pos);
    ret = ioctx.operate("obj", &op);
    ASSERT_EQ(ret, zlog::CLS_ZLOG_OK);
  }

  std::set<uint64_t>::iterator it = filled.begin();
  for (; it != filled.end(); it++) {
    uint64_t pos = *it;
    librados::ObjectWriteOperation op;
    zlog::cls_zlog_fill(op, 100, pos);
    ret = ioctx.operate("obj", &op);
    ASSERT_EQ(ret, zlog::CLS_ZLOG_OK);
  }

  bufferlist bl;
  bl.append("some data");

  // filling a written position yields read-only status
  std::set<uint64_t> written;
  for (int i = 0; i < 100; i++) {
    uint64_t pos = rand();

    if (written.count(pos))
      continue;

    written.insert(pos);
    librados::ObjectWriteOperation op;
    zlog::cls_zlog_write(op, 100, pos, bl);
    ret = ioctx.operate("obj", &op);
    ASSERT_EQ(ret, zlog::CLS_ZLOG_OK);
  }

  it = written.begin();
  for (; it != written.end(); it++) {
    uint64_t pos = *it;
    librados::ObjectWriteOperation op;
    zlog::cls_zlog_fill(op, 100, pos);
    ret = ioctx.operate("obj", &op);
    ASSERT_EQ(ret, zlog::CLS_ZLOG_READ_ONLY);
  }

  // fill doesn't affect max position
  uint64_t pos;
  int status;
  bufferlist bl3;
  librados::ObjectReadOperation op2;
  zlog::cls_zlog_max_position(op2, 100, &pos, &status);
  ret = ioctx.operate("obj", &op2, &bl3);
  ASSERT_EQ(ret, zlog::CLS_ZLOG_OK);
  ASSERT_GT(pos, (unsigned)0);

  op = new_op();
  zlog::cls_zlog_fill(*op, 100, pos + 10);
  ret = ioctx.operate("obj", op);
  ASSERT_EQ(ret, zlog::CLS_ZLOG_OK);

  uint64_t pos2;
  bufferlist bl2;
  librados::ObjectReadOperation op3;
  zlog::cls_zlog_max_position(op3, 100, &pos2, &status);
  ret = ioctx.operate("obj", &op3, &bl2);
  ASSERT_EQ(ret, zlog::CLS_ZLOG_OK);
  ASSERT_EQ(pos, pos2);


  // fails if there is junk entry
  std::map<std::string, bufferlist> vals;
  bl.clear();
  bl.append("j");
  vals["____zlog.pos.00000000000000000099"] = bl;
  ioctx.omap_set("obj", vals);
  op = new_op();
  zlog::cls_zlog_fill(*op, 100, 99);
  ret = ioctx.operate("obj", op);
  ASSERT_EQ(ret, -EIO);

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(ClsZlog, Write) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  // fails to decode input (bad message)
  bufferlist inbl, outbl;
  int ret = ioctx.exec("obj", "zlog", "write", inbl, outbl);
  ASSERT_EQ(ret, -EINVAL);

  // rejects if no epoch has been set
  bufferlist bl2;
  bl2.append("baasdf");
  librados::ObjectWriteOperation *op = new_op();
  zlog::cls_zlog_write(*op, 100, 10, bl2);
  ret = ioctx.operate("obj", op);
  ASSERT_EQ(ret, -ENOENT);

  // set epoch to 100
  op = new_op();
  zlog::cls_zlog_seal(*op, 100);
  ret = ioctx.operate("obj", op);
  ASSERT_EQ(ret, zlog::CLS_ZLOG_OK);

  // try again
  op = new_op();
  zlog::cls_zlog_write(*op, 100, 10, bl2);
  ret = ioctx.operate("obj", op);
  ASSERT_EQ(ret, zlog::CLS_ZLOG_OK);

  // try with smaller epoch
  op = new_op();
  zlog::cls_zlog_write(*op, 0, 20, bl2);
  ret = ioctx.operate("obj", op);
  ASSERT_EQ(ret, zlog::CLS_ZLOG_STALE_EPOCH);

  // try with larger epoch
  op = new_op();
  zlog::cls_zlog_write(*op, 1000, 20, bl2);
  ret = ioctx.operate("obj", op);
  ASSERT_EQ(ret, zlog::CLS_ZLOG_OK);

  op = new_op();
  zlog::cls_zlog_write(*op, 1000, 10, bl2);
  ret = ioctx.operate("obj", op);
  ASSERT_EQ(ret, zlog::CLS_ZLOG_READ_ONLY);

  // new max position should be correct too
  bufferlist bl5;
  uint64_t maxpos5;
  int status5;
  librados::ObjectReadOperation op5;
  zlog::cls_zlog_max_position(op5, 100, &maxpos5, &status5);
  ret = ioctx.operate("obj", &op5, &bl5);
  ASSERT_EQ(ret, zlog::CLS_ZLOG_OK);
  ASSERT_EQ(maxpos5, (unsigned)20);

  bufferlist bl;
  bl.append("some data");
  srand(0);

  // set epoch to 100
  op = new_op();
  zlog::cls_zlog_seal(*op, 100);
  ret = ioctx.operate("obj3", op);
  ASSERT_EQ(ret, zlog::CLS_ZLOG_OK);

  // write then write -> read only status
  uint64_t max = 0;
  std::set<uint64_t> written;
  for (int i = 0; i < 100; i++) {
    uint64_t pos = rand();

    // include pos = 0 first, as it tests the initialization case of the max
    // position in cls_zlog
    if (i == 0)
      pos = 0;

    if (written.count(pos))
      continue;

    if (pos > max)
      max = pos;

    written.insert(pos);
    librados::ObjectWriteOperation op;
    zlog::cls_zlog_write(op, 100, pos, bl);
    ret = ioctx.operate("obj3", &op);
    ASSERT_EQ(ret, zlog::CLS_ZLOG_OK);

    // new max position should be correct too
    bufferlist bl3;
    uint64_t maxpos;
    int status;
    librados::ObjectReadOperation op2;
    zlog::cls_zlog_max_position(op2, 100, &maxpos, &status);
    ret = ioctx.operate("obj3", &op2, &bl3);
    ASSERT_EQ(ret, zlog::CLS_ZLOG_OK);
    
    ASSERT_EQ(maxpos, max);
  }

  std::set<uint64_t>::iterator it = written.begin();
  for (; it != written.end(); it++) {
    uint64_t pos = *it;
    librados::ObjectWriteOperation op;
    zlog::cls_zlog_write(op, 100, pos, bl);
    ret = ioctx.operate("obj3", &op);
    ASSERT_EQ(ret, zlog::CLS_ZLOG_READ_ONLY);
  }

  // a bunch of writes that failed didn't affect max pos
  bufferlist bl4;
  uint64_t pos;
  int status;
  librados::ObjectReadOperation op4;
  zlog::cls_zlog_max_position(op4, 100, &pos, &status);
  ret = ioctx.operate("obj3", &op4, &bl4);
  ASSERT_EQ(ret, zlog::CLS_ZLOG_OK);
  ASSERT_EQ(pos, max);

  // set epoch to 100 for obj2
  op = new_op();
  zlog::cls_zlog_seal(*op, 100);
  ret = ioctx.operate("obj2", op);
  ASSERT_EQ(ret, zlog::CLS_ZLOG_OK);

  // fill then write -> read only status
  std::set<uint64_t> filled;
  for (int i = 0; i < 100; i++) {
    uint64_t pos = rand();

    filled.insert(pos);
    librados::ObjectWriteOperation op;
    zlog::cls_zlog_fill(op, 100, pos);
    ret = ioctx.operate("obj2", &op);
    ASSERT_EQ(ret, zlog::CLS_ZLOG_OK);
  }

  it = filled.begin();
  for (; it != filled.end(); it++) {
    uint64_t pos = *it;
    librados::ObjectWriteOperation op;
    zlog::cls_zlog_write(op, 100, pos, bl);
    ret = ioctx.operate("obj2", &op);
    ASSERT_EQ(ret, zlog::CLS_ZLOG_READ_ONLY);
  }

  // a bunch of writes that failed didn't set max pos
  bufferlist bl3;
  librados::ObjectReadOperation op2;
  zlog::cls_zlog_max_position(op2, 100, &pos, &status);
  ret = ioctx.operate("obj2", &op2, &bl3);
  ASSERT_EQ(ret, -ENOENT);

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(ClsZlog, Read) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  // for these read only ops they'll return -enoent if the object doesn't
  // exist. in practice we could loosen this restriction if there are races
  // that pop up when bootstrapping a log, but really a client shouldn't be
  // trying to read a log until its gotten a tail from the sequencer, and the
  // sequencer won't hand anything out until its initialized the log objects.
  ioctx.create("obj", true);

  // fails to decode input (bad message)
  bufferlist inbl, outbl;
  int ret = ioctx.exec("obj", "zlog", "read", inbl, outbl);
  ASSERT_EQ(ret, -EINVAL);

  // rejects if no epoch has been set
  bufferlist bl;
  librados::ObjectReadOperation *op = new_rop();
  zlog::cls_zlog_read(*op, 100, 10);
  ret = ioctx.operate("obj", op, &bl);
  ASSERT_EQ(ret, -ENOENT);

  // set epoch to 100
  librados::ObjectWriteOperation *wrop = new_op();
  zlog::cls_zlog_seal(*wrop, 100);
  ret = ioctx.operate("obj", wrop);
  ASSERT_EQ(ret, zlog::CLS_ZLOG_OK);

  // try again
  op = new_rop();
  zlog::cls_zlog_read(*op, 100, 10);
  ret = ioctx.operate("obj", op, &bl);
  ASSERT_EQ(ret, zlog::CLS_ZLOG_NOT_WRITTEN);

  // try with smaller epoch
  op = new_rop();
  zlog::cls_zlog_read(*op, 0, 20);
  ret = ioctx.operate("obj", op, &bl);
  ASSERT_EQ(ret, zlog::CLS_ZLOG_STALE_EPOCH);

  srand(0);

  // cannot read from unwritten locations
  for (int i = 0; i < 100; i++) {
    uint64_t pos = rand();
    bufferlist bl;
    librados::ObjectReadOperation op;
    zlog::cls_zlog_read(op, 100, pos);
    ret = ioctx.operate("obj", &op, &bl);
    ASSERT_EQ(ret, zlog::CLS_ZLOG_NOT_WRITTEN);
  }

  // can read stuff that was written
  std::set<uint64_t> written;
  for (int i = 0; i < 100; i++) {
    uint64_t pos = rand();
    if (written.count(pos))
      continue;
    written.insert(pos);
    bufferlist bl;
    bl.append((char*)&pos, sizeof(pos));
    librados::ObjectWriteOperation op;
    zlog::cls_zlog_write(op, 100, pos, bl);
    ret = ioctx.operate("obj", &op);
    ASSERT_EQ(ret, zlog::CLS_ZLOG_OK);
  }

  std::set<uint64_t>::iterator it = written.begin();
  for (; it != written.end(); it++) {
    uint64_t pos = *it;
    bufferlist bl;
    librados::ObjectReadOperation op;
    zlog::cls_zlog_read(op, 100, pos);
    ret = ioctx.operate("obj", &op, &bl);
    ASSERT_EQ(ret, zlog::CLS_ZLOG_OK);
    ASSERT_TRUE(memcmp(bl.c_str(), &pos, sizeof(pos)) == 0);
  }

  ioctx.create("obj2", true);
  wrop = new_op();
  zlog::cls_zlog_seal(*wrop, 100);
  ret = ioctx.operate("obj2", wrop);
  ASSERT_EQ(ret, zlog::CLS_ZLOG_OK);

  // stuff that was filled is invalid when read
  std::set<uint64_t> filled;
  for (int i = 0; i < 100; i++) {
    uint64_t pos = rand();
    filled.insert(pos);
    librados::ObjectWriteOperation op;
    zlog::cls_zlog_fill(op, 100, pos);
    ret = ioctx.operate("obj2", &op);
    ASSERT_EQ(ret, zlog::CLS_ZLOG_OK);
  }

  it = filled.begin();
  for (; it != filled.end(); it++) {
    uint64_t pos = *it;
    bufferlist bl;
    librados::ObjectReadOperation op;
    zlog::cls_zlog_read(op, 100, pos);
    ret = ioctx.operate("obj2", &op, &bl);
    ASSERT_EQ(ret, zlog::CLS_ZLOG_INVALIDATED);
  }

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(ClsZlog, MaxPosition) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  ioctx.create("obj", true);

  // fails to decode input (bad message)
  bufferlist inbl, outbl;
  int ret = ioctx.exec("obj", "zlog", "max_position", inbl, outbl);
  ASSERT_EQ(ret, -EINVAL);

  // set epoch to 100
  librados::ObjectWriteOperation *wrop = new_op();
  zlog::cls_zlog_seal(*wrop, 100);
  ret = ioctx.operate("obj", wrop);
  ASSERT_EQ(ret, zlog::CLS_ZLOG_OK);

  bufferlist bl;
  uint64_t pos;
  int status;
  librados::ObjectReadOperation op;
  zlog::cls_zlog_max_position(op, 100, &pos, &status);
  ret = ioctx.operate("obj", &op, &bl);
  ASSERT_EQ(ret, -ENOENT);

  librados::ObjectWriteOperation *wop = new_op();
  zlog::cls_zlog_write(*wop, 100, 0, bl);
  ret = ioctx.operate("obj", wop);
  ASSERT_EQ(ret, zlog::CLS_ZLOG_OK);

  librados::ObjectReadOperation op2;
  zlog::cls_zlog_max_position(op2, 100, &pos, &status);
  ret = ioctx.operate("obj", &op2, &bl);
  ASSERT_EQ(status, zlog::CLS_ZLOG_OK);
  ASSERT_EQ(ret, zlog::CLS_ZLOG_OK);
  ASSERT_EQ(pos, (unsigned)0);

  wop = new_op();
  zlog::cls_zlog_write(*wop, 100, 50, bl);
  ret = ioctx.operate("obj", wop);
  ASSERT_EQ(ret, zlog::CLS_ZLOG_OK);

  // FIXME: report this problem with bl2 causing pool delete/shutdown to hang.
  // when its sharing the bl above, it hangs. with bl2 its ok.
  bufferlist bl2;
  librados::ObjectReadOperation op3;
  zlog::cls_zlog_max_position(op3, 100, &pos, &status);
  ret = ioctx.operate("obj", &op3, &bl2);
  ASSERT_EQ(status, zlog::CLS_ZLOG_OK);
  ASSERT_EQ(ret, zlog::CLS_ZLOG_OK);
  ASSERT_EQ(pos, (unsigned)50);

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}
