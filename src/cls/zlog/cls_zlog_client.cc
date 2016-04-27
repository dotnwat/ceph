#include <errno.h>

#include "include/types.h"
#include "include/rados/librados.hpp"
#include "cls_zlog_client.h"
#include "cls_zlog_ops.h"

namespace zlog {

void cls_zlog_seal(librados::ObjectWriteOperation& op, uint64_t epoch)
{
  bufferlist in;
  cls_zlog_seal_op call;
  call.epoch = epoch;
  ::encode(call, in);
  op.exec("zlog", "seal", in);
}

void cls_zlog_fill(librados::ObjectWriteOperation& op, uint64_t epoch,
    uint64_t position)
{
  bufferlist in;
  cls_zlog_fill_op call;
  call.epoch = epoch;
  call.position = position;
  ::encode(call, in);
  op.exec("zlog", "fill", in);
}

void cls_zlog_write(librados::ObjectWriteOperation& op, uint64_t epoch,
    uint64_t position, ceph::bufferlist& data)
{
  bufferlist in;
  cls_zlog_write_op call;
  call.epoch = epoch;
  call.position = position;
  call.data = data;
  ::encode(call, in);
  op.exec("zlog", "write", in);
}

void cls_zlog_read(librados::ObjectReadOperation& op, uint64_t epoch,
    uint64_t position)
{
  bufferlist in;
  cls_zlog_read_op call;
  call.epoch = epoch;
  call.position = position;
  ::encode(call, in);
  op.exec("zlog", "read", in);
}

void cls_zlog_trim(librados::ObjectWriteOperation& op, uint64_t epoch,
    uint64_t position)
{
  bufferlist in;
  cls_zlog_trim_op call;
  call.epoch = epoch;
  call.position = position;
  ::encode(call, in);
  op.exec("zlog", "trim", in);
}

class ClsZlogMaxPositionReply : public librados::ObjectOperationCompletion {
 public:
  ClsZlogMaxPositionReply(uint64_t *pposition, int *pret) :
    pposition_(pposition), pret_(pret)
  {}

  void handle_completion(int ret, bufferlist& outbl) {
    if (ret == CLS_ZLOG_OK) {
      try {
        cls_zlog_max_position_ret reply;
        bufferlist::iterator it = outbl.begin();
        ::decode(reply, it);
        *pposition_ = reply.position;
      } catch (buffer::error& err) {
        ret = -EIO;
      }
    }
    *pret_ = ret;
  }

 private:
  uint64_t *pposition_;
  int *pret_;
};

void cls_zlog_max_position(librados::ObjectReadOperation& op, uint64_t epoch,
    uint64_t *pposition, int *pret)
{
  bufferlist in;
  cls_zlog_max_position_op call;
  call.epoch = epoch;
  ::encode(call, in);
  op.exec("zlog", "max_position", in, new ClsZlogMaxPositionReply(pposition, pret));
}

void cls_zlog_set_projection(librados::ObjectWriteOperation& op,
    uint64_t epoch, ceph::bufferlist& data)
{
  cls_zlog_set_projection_op call;
  call.epoch = epoch;
  call.data = data;

  bufferlist inbl;
  ::encode(call, inbl);

  op.exec("zlog", "set_projection", inbl);
}

class GetProjectionReply : public librados::ObjectOperationCompletion {
 public:
  GetProjectionReply(int *pret, uint64_t *pepoch, bufferlist *out) :
    pret_(pret), pepoch_(pepoch), out_(out)
  {}

  void handle_completion(int ret, bufferlist& outbl) {
    if (ret == 0) {
      cls_zlog_get_projection_ret reply;
      try {
        bufferlist::iterator it = outbl.begin();
        ::decode(reply, it);
        if (pepoch_)
          *pepoch_ = reply.epoch;
        *out_ = reply.out;
      } catch (buffer::error& err) {
        ret = -EIO;
      }
    }
    *pret_ = ret;
  }

 private:
  int *pret_;
  uint64_t *pepoch_;
  bufferlist *out_;
};

static void __get_projection(librados::ObjectReadOperation& op,
    uint64_t epoch, bool latest, int *pret, uint64_t *pepoch, bufferlist *out)
{
  cls_zlog_get_projection_op call;
  call.epoch = epoch;
  call.latest = latest;

  bufferlist inbl;
  ::encode(call, inbl);

  op.exec("zlog", "get_projection", inbl,
      new GetProjectionReply(pret, pepoch, out));
}

void cls_zlog_get_latest_projection(librados::ObjectReadOperation& op,
    int *pret, uint64_t *pepoch, bufferlist *out)
{
  __get_projection(op, 0, true, pret, pepoch, out);
}

void cls_zlog_get_projection(librados::ObjectReadOperation& op, int *pret,
    uint64_t epoch, bufferlist *out)
{
  __get_projection(op, epoch, false, pret, NULL, out);
}

}
