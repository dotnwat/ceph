#include <iostream>
#include <rados/buffer.h>
#include <rados/librados.hpp>
#include "zlog.pb.h"
#include "zlog_encoding.h"

using namespace std;

void encode(ceph::buffer::list& bl, google::protobuf::Message& msg) {
  assert(msg.IsInitialized());
  char buf[msg.ByteSize()];
  assert(msg.SerializeToArray(buf, sizeof(buf)));
  bl.append(buf, sizeof(buf));
}

void decode(ceph::buffer::list& bl, google::protobuf::Message* msg) {
  if (!msg->ParseFromString(bl.to_str())) {
    cerr << "decode: unable to decode message" << endl;
    throw buffer::malformed_input("decode: unable to decode message");
  }
  if (!msg->IsInitialized()) {
    cerr << "decode: message is uninitialized" << endl;
    throw buffer::malformed_input("decode: message is uninitialized");
  }
}
