#pragma once
#include "include/rados/objclass.h"
#include "include/rados/buffer.h"
#include "include/rados/librados.hpp"
#include "zlog.pb.h"

namespace cls_zlog {

static inline void encode(ceph::bufferlist& bl, google::protobuf::Message& msg)
{
  assert(msg.IsInitialized());
  char buf[msg.ByteSize()];
  assert(msg.SerializeToArray(buf, sizeof(buf)));
  bl.append(buf, sizeof(buf));
}

static inline bool decode(ceph::bufferlist& bl, google::protobuf::Message *msg)
{
  if (bl.length() == 0) {
    return false;
  }
  if (!msg->ParseFromString(bl.to_str())) {
    CLS_ERR("ERROR: decode: unable to decode message");
    return false;
  }
  if (!msg->IsInitialized()) {
    CLS_ERR("ERROR: decode: message is uninitialized");
    return false;
  }
  return true;
}

}
