#pragma once
#include "include/rados/librados.hpp"

namespace cls_zlog_client {
  void init(librados::ObjectWriteOperation& op, uint32_t entry_size,
      uint32_t stripe_width, uint32_t entries_per_object,
      uint64_t object_id);

  void read(librados::ObjectReadOperation& op, uint64_t position);

  void write(librados::ObjectWriteOperation& op, uint64_t position,
      ceph::bufferlist& data);

  void invalidate(librados::ObjectWriteOperation& op, uint64_t position,
      bool force = false);

  void view_init(librados::ObjectWriteOperation& op, uint32_t entry_size,
      uint32_t stripe_width, uint32_t entries_per_object, uint32_t num_stripes);

  void view_read(librados::ObjectReadOperation& op, uint64_t min_epoch);
}
