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

void cls_zlog_set_projection(librados::ObjectWriteOperation& op)
{
  bufferlist in;
  op.exec("zlog", "set_projection", in);
}

int cls_zlog_get_projection(librados::IoCtx& ioctx, const std::string& oid, uint64_t *pepoch)
{
  bufferlist inbl, outbl;
  int ret = ioctx.exec(oid, "zlog", "get_projection", inbl, outbl);
  if (ret)
    return ret;

  uint64_t epoch;
  try {
    bufferlist::iterator it = outbl.begin();
    ::decode(epoch, it);
    *pepoch = epoch;
  } catch (buffer::error& err) {
    return -EIO;
  }

  return 0;
}

}
