#ifndef CLS_ZLOG_BENCH_CLIENT_H
#define CLS_ZLOG_BENCH_CLIENT_H

#ifdef __CEPH__
# include "include/rados/librados.hpp"
#else
# include <rados/librados.hpp>
#endif

namespace zlog_bench {
  void cls_zlog_bench_append(librados::ObjectWriteOperation& op, uint64_t epoch,
      uint64_t position, ceph::bufferlist& data);

  void cls_zlog_bench_append_init(librados::ObjectWriteOperation& op);

  void cls_zlog_bench_append_check_epoch(librados::ObjectWriteOperation& op,
      uint64_t epoch, uint64_t position, ceph::bufferlist& data);

  void cls_zlog_bench_append_omap_index(librados::ObjectWriteOperation& op,
      uint64_t epoch, uint64_t position, ceph::bufferlist& data);
}

#endif
