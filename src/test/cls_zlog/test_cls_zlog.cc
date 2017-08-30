#include <random>
#include <algorithm>
#include "cls/zlog/cls_zlog_client.h"
#include "gtest/gtest.h"
#include "test/librados/test.h"
#include "zlog.pb.h"
#include "cls/zlog/common.h"

using namespace librados;

class ClsZlogTest : public ::testing::Test {
 protected:
  virtual void SetUp() {
    pool_name = get_temp_pool_name();
    ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
    cluster.ioctx_create(pool_name.c_str(), ioctx);
  }

  virtual void TearDown() {
    ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
  }

  std::string pool_name;
  Rados cluster;
  IoCtx ioctx;
};

static std::unique_ptr<ObjectWriteOperation> new_wop() {
  return std::unique_ptr<ObjectWriteOperation>(new ObjectWriteOperation());
}

static std::unique_ptr<ObjectReadOperation> new_rop() {
  return std::unique_ptr<ObjectReadOperation>(new ObjectReadOperation());
}

static int do_init(librados::IoCtx& ioctx,
    uint32_t entry_size, uint32_t stripe_width,
    uint32_t entries_per_object, uint64_t object_id,
    std::string oid = "obj")
{
  auto op = new_wop();
  cls_zlog_client::init(*op, entry_size, stripe_width,
      entries_per_object, object_id);
  return ioctx.operate(oid, op.get());
}

static int do_read(librados::IoCtx& ioctx,
    uint64_t pos, ceph::bufferlist& bl, std::string oid = "obj")
{
  auto op = new_rop();
  cls_zlog_client::read(*op, pos);
  return ioctx.operate(oid, op.get(), &bl);
}

static int do_write(librados::IoCtx& ioctx,
    uint64_t pos, ceph::bufferlist& bl, std::string oid = "obj")
{
  auto op = new_wop();
  cls_zlog_client::write(*op, pos, bl);
  return ioctx.operate(oid, op.get());
}

static int do_invalidate(librados::IoCtx& ioctx,
    uint64_t pos, bool force, std::string oid = "obj")
{
  auto op = new_wop();
  cls_zlog_client::invalidate(*op, pos, force);
  return ioctx.operate(oid, op.get());
}

TEST_F(ClsZlogTest, InitBadInput) {
  bufferlist inbl, outbl;
  inbl.append("foo", strlen("foo"));
  int ret = ioctx.exec("obj", "zlog", "init", inbl, outbl);
  ASSERT_EQ(ret, -EINVAL);
}

TEST_F(ClsZlogTest, InitMissingMetadata) {
  int ret = ioctx.create("obj", true);
  ASSERT_EQ(ret, 0);

  // missing
  ret = do_init(ioctx, 1, 1, 1, 0);
  ASSERT_EQ(ret, -EIO);
}

TEST_F(ClsZlogTest, InitCorruptMetadata) {
  // corrupt
  bufferlist bl;
  bl.append("foo", strlen("foo"));
  int ret = ioctx.setxattr("obj", "meta", bl);
  ASSERT_EQ(ret, 0);

  ret = do_init(ioctx, 1, 1, 1, 0);
  ASSERT_EQ(ret, -EIO);
}

TEST_F(ClsZlogTest, InitInvalidParams) {
  int ret = do_init(ioctx, 0, 0, 0, 0);
  ASSERT_EQ(ret, -EINVAL);

  ret = do_init(ioctx, 1, 0, 0, 0);
  ASSERT_EQ(ret, -EINVAL);

  ret = do_init(ioctx, 1, 1, 0, 0);
  ASSERT_EQ(ret, -EINVAL);
}

TEST_F(ClsZlogTest, InitMismatchParams) {
  int ret = do_init(ioctx, 1, 1, 1, 0);
  ASSERT_EQ(ret, 0);

  ret = do_init(ioctx, 1, 1, 1, 0);
  ASSERT_EQ(ret, 0);

  ret = do_init(ioctx, 2, 1, 1, 0);
  ASSERT_EQ(ret, -EINVAL);

  ret = do_init(ioctx, 1, 2, 1, 0);
  ASSERT_EQ(ret, -EINVAL);

  ret = do_init(ioctx, 1, 1, 2, 0);
  ASSERT_EQ(ret, -EINVAL);
}

TEST_F(ClsZlogTest, WriteBadInput) {
  bufferlist inbl, outbl;
  inbl.append("foo", strlen("foo"));
  int ret = ioctx.exec("obj", "zlog", "write", inbl, outbl);
  ASSERT_EQ(ret, -EINVAL);
}

TEST_F(ClsZlogTest, WriteDoesntExist) {
  bufferlist bl;
  int ret = do_write(ioctx, 100, bl);
  ASSERT_EQ(ret, -ENOENT);
}

TEST_F(ClsZlogTest, WriteMissingObjectMetadata) {
  int ret = ioctx.create("obj", true);
  ASSERT_EQ(ret, 0);

  // object will exist, but no metadata will exist
  bufferlist bl;
  ret = do_write(ioctx, 100, bl);
  ASSERT_EQ(ret, -EIO);
}

TEST_F(ClsZlogTest, WriteCorruptMetadata) {
  int ret = ioctx.create("obj", true);
  ASSERT_EQ(ret, 0);

  bufferlist bl;
  bl.append("foo", strlen("foo"));
  ret = ioctx.setxattr("obj", "meta", bl);
  ASSERT_EQ(ret, 0);

  // object will exist with corrupt metadata
  ret = do_write(ioctx, 100, bl);
  ASSERT_EQ(ret, -EIO);
}

TEST_F(ClsZlogTest, WriteInvalidMetadata) {
  int ret = ioctx.create("obj", true);
  ASSERT_EQ(ret, 0);

  zlog_proto::ObjectMeta omd;
  omd.mutable_params()->set_entry_size(1);
  omd.mutable_params()->set_stripe_width(1);
  omd.mutable_params()->set_entries_per_object(1);
  omd.set_object_id(0);

  bufferlist bl;
  cls_zlog::encode(bl, omd);
  ret = ioctx.setxattr("obj", "meta", bl);
  ASSERT_EQ(ret, 0);

  bl.clear();
  ret = do_write(ioctx, 0, bl);
  ASSERT_EQ(ret, 0);

  omd.mutable_params()->set_entry_size(0);
  bl.clear();
  cls_zlog::encode(bl, omd);
  ret = ioctx.setxattr("obj", "meta", bl);
  ASSERT_EQ(ret, 0);

  bl.clear();
  ret = do_write(ioctx, 0, bl);
  ASSERT_EQ(ret, -EIO);
}

TEST_F(ClsZlogTest, WriteWildWrite) {
  auto es = 1;
  for (auto sw = 1; sw < 5; sw++) {
    for (auto eo = 1; eo < 10; eo++) {
      auto maxpos = sw * eo * 3;
      for (auto pos = 0; pos < maxpos; pos++) {
        std::stringstream oid;
        bufferlist bl;
        const uint64_t stripe_num = pos / sw;
        const uint64_t stripepos = pos % sw;
        const uint64_t objectsetno = stripe_num / eo;
        const uint64_t objectno = objectsetno * sw + stripepos;
        oid << sw << "." << eo << "." << objectno;

        // object no and position match
        int ret = do_init(ioctx, es, sw, eo,
            objectno, oid.str());
        ASSERT_EQ(ret, 0);

        // pos is set for next object set
        ret = do_write(ioctx, pos+(eo*sw), bl, oid.str());
        ASSERT_EQ(ret, -EFAULT);
      }
    }
  }
}

TEST_F(ClsZlogTest, WriteEntryExists) {
  int ret = do_init(ioctx, 1024, 1, 10, 0);
  ASSERT_EQ(ret, 0);

  char buf[4096];
  bufferlist bl;
  bl.append(buf, 100);

  ret = do_write(ioctx, 3, bl);
  ASSERT_EQ(ret, 0);

  ret = do_write(ioctx, 3, bl);
  ASSERT_EQ(ret, -EEXIST);

  ret = do_write(ioctx, 0, bl);
  ASSERT_EQ(ret, 0);

  ret = do_write(ioctx, 0, bl);
  ASSERT_EQ(ret, -EEXIST);

  ret = do_write(ioctx, 1, bl);
  ASSERT_EQ(ret, 0);

  ret = do_write(ioctx, 1, bl);
  ASSERT_EQ(ret, -EEXIST);

  ret = do_write(ioctx, 5, bl);
  ASSERT_EQ(ret, 0);

  ret = do_write(ioctx, 5, bl);
  ASSERT_EQ(ret, -EEXIST);
}

TEST_F(ClsZlogTest, WriteEntryTooLarge) {
  int ret = do_init(ioctx, 1024, 1, 10, 0);
  ASSERT_EQ(ret, 0);

  char buf[4096];
  bufferlist bl;

  // entry larger than configured
  bl.append(buf, sizeof(buf));
  ret = do_write(ioctx, 0, bl);
  ASSERT_EQ(ret, -EFBIG);

  // smaller size is ok
  bl.clear();
  bl.append(buf, 100);
  ret = do_write(ioctx, 0, bl);
  ASSERT_EQ(ret, 0);

  // still rejects
  bl.clear();
  bl.append(buf, sizeof(buf));
  ret = do_write(ioctx, 1, bl);
  ASSERT_EQ(ret, -EFBIG);
}

TEST_F(ClsZlogTest, WriteHole) {
  int ret = do_init(ioctx, 1024, 1, 10, 0);
  ASSERT_EQ(ret, 0);

  char buf[4096];
  bufferlist bl;

  bl.append(buf, 100);
  ret = do_write(ioctx, 8, bl);
  ASSERT_EQ(ret, 0);

  bl.append(buf, 100);
  ret = do_write(ioctx, 3, bl);
  ASSERT_EQ(ret, 0);
}

TEST_F(ClsZlogTest, WritePastEof) {
  int ret = do_init(ioctx, 1024, 1, 10, 0);
  ASSERT_EQ(ret, 0);

  char buf[4096];
  bufferlist bl;

  bl.append(buf, 100);
  ret = do_write(ioctx, 4, bl);
  ASSERT_EQ(ret, 0);

  bl.append(buf, 100);
  ret = do_write(ioctx, 7, bl);
  ASSERT_EQ(ret, 0);
}

TEST_F(ClsZlogTest, WriteInvalidatedPos) {
  int ret = do_init(ioctx, 1024, 1, 10, 0);
  ASSERT_EQ(ret, 0);

  char buf[4096];
  bufferlist bl;

  bl.append(buf, 100);
  ret = do_write(ioctx, 4, bl);
  ASSERT_EQ(ret, 0);

  ret = do_invalidate(ioctx, 4, true);
  ASSERT_EQ(ret, 0);

  ret = do_write(ioctx, 4, bl);
  ASSERT_EQ(ret, -EEXIST);

  ret = do_invalidate(ioctx, 7, false);
  ASSERT_EQ(ret, 0);

  ret = do_write(ioctx, 7, bl);
  ASSERT_EQ(ret, -EEXIST);
}

TEST_F(ClsZlogTest, ReadBadInput) {
  /*
   * For reads, if the object doesn't exist Ceph will return ENOENT before the
   * cls methods are evaluated.
   */
  bufferlist inbl, outbl;
  inbl.append("foo", strlen("foo"));
  int ret = ioctx.exec("obj", "zlog", "read", inbl, outbl);
  ASSERT_EQ(ret, -ENOENT);

  ret = ioctx.create("obj", true);
  ASSERT_EQ(ret, 0);

  // here the object does exist, so we trigger the invalid message check
  ret = ioctx.exec("obj", "zlog", "read", inbl, outbl);
  ASSERT_EQ(ret, -EINVAL);
}

TEST_F(ClsZlogTest, ReadDoesntExist) {
  bufferlist bl;
  int ret = do_read(ioctx, 100, bl);
  ASSERT_EQ(ret, -ENOENT);
}

TEST_F(ClsZlogTest, ReadMissingObjectMetadata) {
  int ret = ioctx.create("obj", true);
  ASSERT_EQ(ret, 0);

  // object will exist, but no metadata will exist
  bufferlist bl;
  ret = do_read(ioctx, 100, bl);
  ASSERT_EQ(ret, -EIO);
}

TEST_F(ClsZlogTest, ReadCorruptMetadata) {
  int ret = ioctx.create("obj", true);
  ASSERT_EQ(ret, 0);

  bufferlist bl;
  bl.append("foo", strlen("foo"));
  ret = ioctx.setxattr("obj", "meta", bl);
  ASSERT_EQ(ret, 0);

  // object will exist with corrupt metadata
  ret = do_read(ioctx, 100, bl);
  ASSERT_EQ(ret, -EIO);
}

TEST_F(ClsZlogTest, ReadInvalidMetadata) {
  int ret = ioctx.create("obj", true);
  ASSERT_EQ(ret, 0);

  zlog_proto::ObjectMeta omd;
  omd.mutable_params()->set_entry_size(1);
  omd.mutable_params()->set_stripe_width(1);
  omd.mutable_params()->set_entries_per_object(1);
  omd.set_object_id(0);

  bufferlist bl;
  cls_zlog::encode(bl, omd);
  ret = ioctx.setxattr("obj", "meta", bl);
  ASSERT_EQ(ret, 0);

  bl.clear();
  ret = do_read(ioctx, 0, bl);
  ASSERT_EQ(ret, zlog_proto::ReadOp::UNWRITTEN);

  omd.mutable_params()->set_entry_size(0);
  bl.clear();
  cls_zlog::encode(bl, omd);
  ret = ioctx.setxattr("obj", "meta", bl);
  ASSERT_EQ(ret, 0);

  bl.clear();
  ret = do_read(ioctx, 0, bl);
  ASSERT_EQ(ret, -EIO);
}

TEST_F(ClsZlogTest, ReadWildRead) {
  auto es = 1;
  for (auto sw = 1; sw < 5; sw++) {
    for (auto eo = 1; eo < 10; eo++) {
      auto maxpos = sw * eo * 3;
      for (auto pos = 0; pos < maxpos; pos++) {
        std::stringstream oid;
        bufferlist bl;
        const uint64_t stripe_num = pos / sw;
        const uint64_t stripepos = pos % sw;
        const uint64_t objectsetno = stripe_num / eo;
        const uint64_t objectno = objectsetno * sw + stripepos;
        oid << sw << "." << eo << "." << objectno;

        // object no and position match
        int ret = do_init(ioctx, es, sw, eo,
            objectno, oid.str());
        ASSERT_EQ(ret, 0);

        // pos is set for next object set
        ret = do_read(ioctx, pos+(eo*sw), bl, oid.str());
        ASSERT_EQ(ret, -EFAULT);
      }
    }
  }
}

TEST_F(ClsZlogTest, ReadOk) {
  int ret = do_init(ioctx, 1024, 1, 10, 0);
  ASSERT_EQ(ret, 0);

  char buf[1024];
  bufferlist bl;

  bl.append(buf, 100);
  ret = do_write(ioctx, 0, bl);
  ASSERT_EQ(ret, 0);

  bufferlist bl2;
  ret = do_read(ioctx, 0, bl2);
  ASSERT_EQ(ret, zlog_proto::ReadOp::OK);

  ASSERT_TRUE(memcmp(bl.c_str(), bl2.c_str(), 100) == 0);
}

TEST_F(ClsZlogTest, ReadUnwrittenHole) {
  int ret = do_init(ioctx, 1024, 1, 10, 0);
  ASSERT_EQ(ret, 0);

  char buf[1024];
  bufferlist bl;

  bl.append(buf, 100);
  ret = do_write(ioctx, 8, bl);
  ASSERT_EQ(ret, 0);

  bufferlist bl2;
  ret = do_read(ioctx, 3, bl2);
  ASSERT_EQ(ret, zlog_proto::ReadOp::UNWRITTEN);
}

TEST_F(ClsZlogTest, ReadUnwrittenPastEof) {
  int ret = do_init(ioctx, 1024, 1, 10, 0);
  ASSERT_EQ(ret, 0);

  char buf[1024];
  bufferlist bl;

  bl.append(buf, 100);
  ret = do_write(ioctx, 0, bl);
  ASSERT_EQ(ret, 0);

  bufferlist bl2;
  ret = do_read(ioctx, 5, bl2);
  ASSERT_EQ(ret, zlog_proto::ReadOp::UNWRITTEN);
}

TEST_F(ClsZlogTest, ReadInvalidatedPos) {
  int ret = do_init(ioctx, 1024, 1, 10, 0);
  ASSERT_EQ(ret, 0);

  char buf[4096];
  bufferlist bl;

  bl.append(buf, 100);
  ret = do_write(ioctx, 4, bl);
  ASSERT_EQ(ret, 0);

  bl.clear();
  ret = do_read(ioctx, 4, bl);
  ASSERT_EQ(ret, 0);

  ret = do_invalidate(ioctx, 4, true);
  ASSERT_EQ(ret, 0);

  bl.clear();
  ret = do_read(ioctx, 4, bl);
  ASSERT_EQ(ret, zlog_proto::ReadOp::INVALID);

  bl.clear();
  ret = do_read(ioctx, 7, bl);
  ASSERT_EQ(ret, zlog_proto::ReadOp::UNWRITTEN);

  ret = do_invalidate(ioctx, 7, false);
  ASSERT_EQ(ret, 0);

  bl.clear();
  ret = do_read(ioctx, 7, bl);
  ASSERT_EQ(ret, zlog_proto::ReadOp::INVALID);
}

TEST_F(ClsZlogTest, InvalidateBadInput) {
  bufferlist inbl, outbl;
  inbl.append("foo", strlen("foo"));
  int ret = ioctx.exec("obj", "zlog", "invalidate", inbl, outbl);
  ASSERT_EQ(ret, -EINVAL);
}

TEST_F(ClsZlogTest, InvalidateDoesntExist) {
  int ret = do_invalidate(ioctx, 100, false);
  ASSERT_EQ(ret, -ENOENT);
}

TEST_F(ClsZlogTest, InvalidateMissingObjectMetadata) {
  int ret = ioctx.create("obj", true);
  ASSERT_EQ(ret, 0);

  // object will exist, but no metadata will exist
  ret = do_invalidate(ioctx, 100, false);
  ASSERT_EQ(ret, -EIO);
}

TEST_F(ClsZlogTest, InvalidateCorruptMetadata) {
  int ret = ioctx.create("obj", true);
  ASSERT_EQ(ret, 0);

  bufferlist bl;
  bl.append("foo", strlen("foo"));
  ret = ioctx.setxattr("obj", "meta", bl);
  ASSERT_EQ(ret, 0);

  // object will exist with corrupt metadata
  ret = do_invalidate(ioctx, 100, false);
  ASSERT_EQ(ret, -EIO);
}

TEST_F(ClsZlogTest, InvalidateInvalidMetadata) {
  int ret = ioctx.create("obj", true);
  ASSERT_EQ(ret, 0);

  zlog_proto::ObjectMeta omd;
  omd.mutable_params()->set_entry_size(1);
  omd.mutable_params()->set_stripe_width(1);
  omd.mutable_params()->set_entries_per_object(1);
  omd.set_object_id(0);

  bufferlist bl;
  cls_zlog::encode(bl, omd);
  ret = ioctx.setxattr("obj", "meta", bl);
  ASSERT_EQ(ret, 0);

  ret = do_invalidate(ioctx, 0, false);
  ASSERT_EQ(ret, 0);

  omd.mutable_params()->set_entry_size(0);
  bl.clear();
  cls_zlog::encode(bl, omd);
  ret = ioctx.setxattr("obj", "meta", bl);
  ASSERT_EQ(ret, 0);

  ret = do_invalidate(ioctx, 0, false);
  ASSERT_EQ(ret, -EIO);
}

TEST_F(ClsZlogTest, ParamSweep) {
  std::vector<unsigned long> ess{1, 2, 1023, 1024};
  std::vector<unsigned long> ess2{0, 1, 2, 1023, 1024};
  int msw = 10;
  int meo = 10;
  int nst = 3;

  // init objects
  for (auto es : ess2) {
    for (auto sw = 0; sw < msw; sw++) {
      for (auto eo = 0; eo < meo; eo++) {
        auto maxpos = sw * eo * nst;
        for (auto pos = 0; pos < maxpos; pos++) {
          std::stringstream oid;
          const uint64_t stripe_num = pos / sw;
          const uint64_t stripepos = pos % sw;
          const uint64_t objectsetno = stripe_num / eo;
          const uint64_t objectno = objectsetno * sw + stripepos;
          oid << es << "." << sw << "." << eo << "." << objectno;
          int ret = do_init(ioctx, es, sw, eo, objectno, oid.str());
          if (es == 0 || sw == 0 || eo == 0) {
            ASSERT_EQ(ret, -EINVAL);
          } else {
            ASSERT_EQ(ret, 0);
          }
        }
      }
    }
  }

  // every position should be unwritten
  for (auto es : ess) {
    for (auto sw = 1; sw < msw; sw++) {
      for (auto eo = 1; eo < meo; eo++) {
        auto maxpos = sw * eo * nst;
        for (auto pos = 0; pos < maxpos; pos++) {
          std::stringstream oid;
          const uint64_t stripe_num = pos / sw;
          const uint64_t stripepos = pos % sw;
          const uint64_t objectsetno = stripe_num / eo;
          const uint64_t objectno = objectsetno * sw + stripepos;
          oid << es << "." << sw << "." << eo << "." << objectno;
          bufferlist bl;
          int ret = do_read(ioctx, pos, bl, oid.str());
          ASSERT_EQ(ret, zlog_proto::ReadOp::UNWRITTEN);
        }
      }
    }
  }

  // write every position randomly
  for (auto es : ess) {
    for (auto sw = 1; sw < msw; sw++) {
      for (auto eo = 1; eo < meo; eo++) {
        auto maxpos = sw * eo * nst;
        std::vector<uint64_t> poss;
        for (auto pos = 0; pos < maxpos; pos++) {
          poss.push_back(pos);
        }
        auto rng = std::default_random_engine {};
        std::shuffle(std::begin(poss), std::end(poss), rng);
        for (uint32_t pos : poss) {
          std::stringstream oid;
          const uint64_t stripe_num = pos / sw;
          const uint64_t stripepos = pos % sw;
          const uint64_t objectsetno = stripe_num / eo;
          const uint64_t objectno = objectsetno * sw + stripepos;
          oid << es << "." << sw << "." << eo << "." << objectno;
          bufferlist bl;
          bl.append((char*)&pos, std::min(es, sizeof(pos)));
          int ret = do_write(ioctx, pos, bl, oid.str());
          ASSERT_EQ(ret, 0);
        }
      }
    }
  }

  // verify every written position
  for (auto es : ess) {
    for (auto sw = 1; sw < msw; sw++) {
      for (auto eo = 1; eo < meo; eo++) {
        auto maxpos = sw * eo * nst;
        std::vector<uint64_t> poss;
        for (auto pos = 0; pos < maxpos; pos++) {
          poss.push_back(pos);
        }
        auto rng = std::default_random_engine {};
        std::shuffle(std::begin(poss), std::end(poss), rng);
        for (uint32_t pos : poss) {
          std::stringstream oid;
          const uint64_t stripe_num = pos / sw;
          const uint64_t stripepos = pos % sw;
          const uint64_t objectsetno = stripe_num / eo;
          const uint64_t objectno = objectsetno * sw + stripepos;
          oid << es << "." << sw << "." << eo << "." << objectno;
          bufferlist bl;
          int ret = do_read(ioctx, pos, bl, oid.str());
          ASSERT_EQ(ret, 0);

          uint32_t pos2 = 0;
          memcpy(&pos2, bl.c_str(), std::min(es, sizeof(pos)));
          ASSERT_EQ(pos, pos2);
        }
      }
    }
  }

  // fill them all
  for (auto es : ess) {
    for (auto sw = 1; sw < msw; sw++) {
      for (auto eo = 1; eo < meo; eo++) {
        auto maxpos = sw * eo * nst;
        std::vector<uint64_t> poss;
        for (auto pos = 0; pos < maxpos; pos++) {
          poss.push_back(pos);
        }
        auto rng = std::default_random_engine {};
        std::shuffle(std::begin(poss), std::end(poss), rng);
        for (uint32_t pos : poss) {
          std::stringstream oid;
          const uint64_t stripe_num = pos / sw;
          const uint64_t stripepos = pos % sw;
          const uint64_t objectsetno = stripe_num / eo;
          const uint64_t objectno = objectsetno * sw + stripepos;
          oid << es << "." << sw << "." << eo << "." << objectno;

          int ret = do_invalidate(ioctx, pos, false, oid.str());
          ASSERT_EQ(ret, -EROFS);

          bufferlist bl;
          ret = do_read(ioctx, pos, bl, oid.str());
          ASSERT_EQ(ret, 0);

          ret = do_invalidate(ioctx, pos, true, oid.str());
          ASSERT_EQ(ret, 0);

          ret = do_read(ioctx, pos, bl, oid.str());
          ASSERT_EQ(ret, zlog_proto::ReadOp::INVALID);

          ret = do_write(ioctx, pos, bl, oid.str());
          ASSERT_EQ(ret, -EEXIST);
        }
      }
    }
  }
}

TEST_F(ClsZlogTest, InvalidateWild) {
  auto es = 1;
  for (auto sw = 1; sw < 5; sw++) {
    for (auto eo = 1; eo < 10; eo++) {
      auto maxpos = sw * eo * 3;
      for (auto pos = 0; pos < maxpos; pos++) {
        std::stringstream oid;
        const uint64_t stripe_num = pos / sw;
        const uint64_t stripepos = pos % sw;
        const uint64_t objectsetno = stripe_num / eo;
        const uint64_t objectno = objectsetno * sw + stripepos;
        oid << sw << "." << eo << "." << objectno;

        // object no and position match
        int ret = do_init(ioctx, es, sw, eo,
            objectno, oid.str());
        ASSERT_EQ(ret, 0);

        // pos is set for next object set
        ret = do_invalidate(ioctx, pos+(eo*sw), false, oid.str());
        ASSERT_EQ(ret, -EFAULT);
      }
    }
  }
}

TEST_F(ClsZlogTest, InvalidateNoForcePastEof) {
  int ret = do_init(ioctx, 100, 1, 10, 0);
  ASSERT_EQ(ret, 0);

  char buf[1024];
  bufferlist bl;

  // write at 3
  bl.append(buf, 100);
  ret = do_write(ioctx, 3, bl);
  ASSERT_EQ(ret, 0);

  // read at 5; unwritten
  bl.clear();
  ret = do_read(ioctx, 5, bl);
  ASSERT_EQ(ret, zlog_proto::ReadOp::UNWRITTEN);

  // invalidate 5
  ret = do_invalidate(ioctx, 5, false);
  ASSERT_EQ(ret, 0);

  // read at 5; invalid
  ret = do_read(ioctx, 5, bl);
  ASSERT_EQ(ret, zlog_proto::ReadOp::INVALID);
}

TEST_F(ClsZlogTest, InvalidateNoForceHole) {
  int ret = do_init(ioctx, 100, 1, 10, 0);
  ASSERT_EQ(ret, 0);

  char buf[1024];
  bufferlist bl;

  // write at 7
  bl.append(buf, 100);
  ret = do_write(ioctx, 7, bl);
  ASSERT_EQ(ret, 0);

  // read at 3; unwritten
  bl.clear();
  ret = do_read(ioctx, 3, bl);
  ASSERT_EQ(ret, zlog_proto::ReadOp::UNWRITTEN);

  // invalidate 3
  ret = do_invalidate(ioctx, 3, false);
  ASSERT_EQ(ret, 0);

  // read at 3; invalid
  ret = do_read(ioctx, 3, bl);
  ASSERT_EQ(ret, zlog_proto::ReadOp::INVALID);
}

TEST_F(ClsZlogTest, InvalidateNoForceAlreadyInvalid) {
  int ret = do_init(ioctx, 100, 1, 10, 0);
  ASSERT_EQ(ret, 0);

  // read at 5; unwritten
  bufferlist bl;
  ret = do_read(ioctx, 5, bl);
  ASSERT_EQ(ret, zlog_proto::ReadOp::UNWRITTEN);

  ret = do_invalidate(ioctx, 5, false);
  ASSERT_EQ(ret, 0);

  // read at 5; invalid
  ret = do_read(ioctx, 5, bl);
  ASSERT_EQ(ret, zlog_proto::ReadOp::INVALID);

  ret = do_invalidate(ioctx, 5, false);
  ASSERT_EQ(ret, 0);

  // read at 5; still invalid
  ret = do_read(ioctx, 5, bl);
  ASSERT_EQ(ret, zlog_proto::ReadOp::INVALID);
}

TEST_F(ClsZlogTest, InvalidateNoForceReadOnly) {
  int ret = do_init(ioctx, 100, 1, 10, 0);
  ASSERT_EQ(ret, 0);

  char buf[1024];
  bufferlist bl;

  // write at 7
  bl.append(buf, 100);
  ret = do_write(ioctx, 7, bl);
  ASSERT_EQ(ret, 0);

  // invalidate 7
  ret = do_invalidate(ioctx, 7, false);
  ASSERT_EQ(ret, -EROFS);
}

TEST_F(ClsZlogTest, InvalidateForcePastEof) {
  int ret = do_init(ioctx, 100, 1, 10, 0);
  ASSERT_EQ(ret, 0);

  char buf[1024];
  bufferlist bl;

  // write at 3
  bl.append(buf, 100);
  ret = do_write(ioctx, 3, bl);
  ASSERT_EQ(ret, 0);

  // read at 5; unwritten
  bl.clear();
  ret = do_read(ioctx, 5, bl);
  ASSERT_EQ(ret, zlog_proto::ReadOp::UNWRITTEN);

  // invalidate 5
  ret = do_invalidate(ioctx, 5, true);
  ASSERT_EQ(ret, 0);

  // read at 5; invalid
  ret = do_read(ioctx, 5, bl);
  ASSERT_EQ(ret, zlog_proto::ReadOp::INVALID);
}

TEST_F(ClsZlogTest, InvalidateForceUnwrittenHole) {
  int ret = do_init(ioctx, 100, 1, 10, 0);
  ASSERT_EQ(ret, 0);

  char buf[1024];
  bufferlist bl;

  // write at 7
  bl.append(buf, 100);
  ret = do_write(ioctx, 7, bl);
  ASSERT_EQ(ret, 0);

  // read at 3; unwritten
  bl.clear();
  ret = do_read(ioctx, 3, bl);
  ASSERT_EQ(ret, zlog_proto::ReadOp::UNWRITTEN);

  // invalidate 3
  ret = do_invalidate(ioctx, 3, true);
  ASSERT_EQ(ret, 0);

  // read at 3; invalid
  ret = do_read(ioctx, 3, bl);
  ASSERT_EQ(ret, zlog_proto::ReadOp::INVALID);
}

TEST_F(ClsZlogTest, InvalidateForceWritten) {
  int ret = do_init(ioctx, 100, 1, 10, 0);
  ASSERT_EQ(ret, 0);

  char buf[1024];
  bufferlist bl;

  // write at 7
  bl.append(buf, 100);
  ret = do_write(ioctx, 7, bl);
  ASSERT_EQ(ret, 0);

  // invalidate 7
  ret = do_invalidate(ioctx, 7, false);
  ASSERT_EQ(ret, -EROFS);

  bl.clear();
  ret = do_read(ioctx, 7, bl);
  ASSERT_EQ(ret, 0);

  // invalidate 7; forced
  ret = do_invalidate(ioctx, 7, true);
  ASSERT_EQ(ret, 0);

  ret = do_read(ioctx, 7, bl);
  ASSERT_EQ(ret, zlog_proto::ReadOp::INVALID);
}

TEST_F(ClsZlogTest, InvalidateForceInvalid) {
  int ret = do_init(ioctx, 100, 1, 10, 0);
  ASSERT_EQ(ret, 0);

  // read at 5; unwritten
  bufferlist bl;
  ret = do_read(ioctx, 5, bl);
  ASSERT_EQ(ret, zlog_proto::ReadOp::UNWRITTEN);

  ret = do_invalidate(ioctx, 5, false);
  ASSERT_EQ(ret, 0);

  // read at 5; invalid
  ret = do_read(ioctx, 5, bl);
  ASSERT_EQ(ret, zlog_proto::ReadOp::INVALID);

  ret = do_invalidate(ioctx, 5, true);
  ASSERT_EQ(ret, 0);

  // read at 5; still invalid
  ret = do_read(ioctx, 5, bl);
  ASSERT_EQ(ret, zlog_proto::ReadOp::INVALID);
}

TEST_F(ClsZlogTest, ViewInitInvalidOp) {
}
