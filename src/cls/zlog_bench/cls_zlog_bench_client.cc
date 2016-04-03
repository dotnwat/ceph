#include <errno.h>
#include "include/types.h"
#include "include/rados/librados.hpp"
#include "cls_zlog_bench_client.h"
#include "cls_zlog_bench_ops.h"

namespace zlog_bench {
  void cls_zlog_bench_append(librados::ObjectWriteOperation& op, uint64_t epoch,
      uint64_t position, ceph::bufferlist& data)
  {
    bufferlist in;
    cls_zlog_bench_append_op call;
    call.epoch = epoch;
    call.position = position;
    call.data = data;
    ::encode(call, in);
    op.exec("zlog_bench", "append", in);
  }

  void cls_zlog_bench_append_init(librados::ObjectWriteOperation& op)
  {
    bufferlist in;
    op.exec("zlog_bench", "append_init", in);
  }

  void cls_zlog_bench_append_check_epoch(librados::ObjectWriteOperation& op,
      uint64_t epoch, uint64_t position, ceph::bufferlist& data)
  {
    bufferlist in;
    cls_zlog_bench_append_op call;
    call.epoch = epoch;
    call.position = position;
    call.data = data;
    ::encode(call, in);
    op.exec("zlog_bench", "append_check_epoch", in);
  }

  void cls_zlog_bench_append_omap_index(librados::ObjectWriteOperation& op,
      uint64_t epoch, uint64_t position, ceph::bufferlist& data)
  {
    bufferlist in;
    cls_zlog_bench_append_op call;
    call.epoch = epoch;
    call.position = position;
    call.data = data;
    ::encode(call, in);
    op.exec("zlog_bench", "append_omap_index", in);
  }
}
