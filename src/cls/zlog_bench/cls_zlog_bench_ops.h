#ifndef CLS_ZLOG_BENCH_OPS_H
#define CLS_ZLOG_BENCH_OPS_H

#include "cls_zlog_bench_client.h"

using namespace zlog_bench;

struct cls_zlog_bench_append_op {
  uint64_t epoch;
  uint64_t position;
  bufferlist data;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(epoch, bl);
    ::encode(position, bl);
    ::encode(data, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(epoch, bl);
    ::decode(position, bl);
    ::decode(data, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_zlog_bench_append_op)

#endif
