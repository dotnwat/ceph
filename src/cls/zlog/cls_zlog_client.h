#ifndef CLS_ZLOG_CLIENT_H
#define CLS_ZLOG_CLIENT_H

#ifdef __CEPH__
# include "include/rados/librados.hpp"
#else
# include <rados/librados.hpp>
#endif

namespace zlog {

  enum {
    CLS_ZLOG_OK            = 0x00,
    CLS_ZLOG_STALE_EPOCH   = 0x01,
    CLS_ZLOG_READ_ONLY     = 0x02,
    CLS_ZLOG_NOT_WRITTEN   = 0x03,
    CLS_ZLOG_INVALIDATED   = 0x04,
    CLS_ZLOG_INVALID_EPOCH = 0x05,
  };

  // version 1

  void cls_zlog_seal(librados::ObjectWriteOperation& op, uint64_t epoch);

  void cls_zlog_fill(librados::ObjectWriteOperation& op, uint64_t epoch,
      uint64_t position);

  void cls_zlog_write(librados::ObjectWriteOperation& op, uint64_t epoch,
      uint64_t position, ceph::bufferlist& data);

  void cls_zlog_read(librados::ObjectReadOperation& op, uint64_t epoch,
      uint64_t position);

  void cls_zlog_trim(librados::ObjectWriteOperation& op, uint64_t epoch,
      uint64_t position);

  void cls_zlog_max_position(librados::ObjectReadOperation& op, uint64_t epoch,
      uint64_t *pposition, int *pret);

  // version 2

  void cls_zlog_seal_v2(librados::ObjectWriteOperation& op, uint64_t epoch);

  void cls_zlog_fill_v2(librados::ObjectWriteOperation& op, uint64_t epoch,
      uint64_t position);

  void cls_zlog_write_v2(librados::ObjectWriteOperation& op, uint64_t epoch,
      uint64_t position, ceph::bufferlist& data);

  void cls_zlog_read_v2(librados::ObjectReadOperation& op, uint64_t epoch,
      uint64_t position);

  void cls_zlog_trim_v2(librados::ObjectWriteOperation& op, uint64_t epoch,
      uint64_t position);

  void cls_zlog_max_position_v2(librados::ObjectReadOperation& op, uint64_t epoch,
      uint64_t *pposition, int *pret);

  // projection management

  void cls_zlog_set_projection(librados::ObjectWriteOperation& op,
      uint64_t epoch, ceph::bufferlist& data);

  void cls_zlog_get_latest_projection(librados::ObjectReadOperation& op,
      int *pret, uint64_t *pepoch, ceph::bufferlist *out);

  void cls_zlog_get_projection(librados::ObjectReadOperation& op,
      int *pret, uint64_t epoch, ceph::bufferlist *out);
}

#endif
