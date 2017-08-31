#include "common.h"

namespace cls_zlog_client {

void init(librados::ObjectWriteOperation& op, uint32_t entry_size,
    uint32_t stripe_width, uint32_t entries_per_object,
    uint64_t object_id)
{
  zlog_proto::InitOp call;
  call.mutable_params()->set_entry_size(entry_size);
  call.mutable_params()->set_stripe_width(stripe_width);
  call.mutable_params()->set_entries_per_object(entries_per_object);
  call.set_object_id(object_id);
  ceph::bufferlist in;
  cls_zlog::encode(in, call);
  op.exec("zlog", "init", in);
}

void read(librados::ObjectReadOperation& op, uint64_t position)
{
  zlog_proto::ReadOp call;
  call.set_position(position);
  ceph::bufferlist in;
  cls_zlog::encode(in, call);
  op.exec("zlog", "read", in);
}

void write(librados::ObjectWriteOperation& op, uint64_t position,
    ceph::bufferlist& data)
{
  zlog_proto::WriteOp call;
  call.set_position(position);
  call.set_data(data.c_str(), data.length());
  ceph::bufferlist in;
  cls_zlog::encode(in, call);
  op.exec("zlog", "write", in);
}

void invalidate(librados::ObjectWriteOperation& op, uint64_t position,
    bool force)
{
  zlog_proto::InvalidateOp call;
  call.set_position(position);
  call.set_force(force);
  ceph::bufferlist in;
  cls_zlog::encode(in, call);
  op.exec("zlog", "invalidate", in);
}

void view_init(librados::ObjectWriteOperation& op, uint32_t entry_size,
    uint32_t stripe_width, uint32_t entries_per_object, uint32_t num_stripes)
{
  zlog_proto::ViewInitOp call;
  call.mutable_params()->set_entry_size(entry_size);
  call.mutable_params()->set_stripe_width(stripe_width);
  call.mutable_params()->set_entries_per_object(entries_per_object);
  call.set_num_stripes(num_stripes);
  ceph::bufferlist in;
  cls_zlog::encode(in, call);
  op.exec("zlog", "view_init", in);
}

void view_read(librados::ObjectReadOperation& op, uint64_t min_epoch)
{
  zlog_proto::ViewReadOp call;
  call.set_min_epoch(min_epoch);
  ceph::bufferlist in;
  cls_zlog::encode(in, call);
  op.exec("zlog", "view_read", in);
}

}
