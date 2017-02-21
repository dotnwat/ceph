#include <rados/buffer.h>
#include "zlog.pb.h"

void encode(ceph::bufferlist& bl, google::protobuf::Message& msg);
void decode(ceph::bufferlist& bl, google::protobuf::Message* msg);
