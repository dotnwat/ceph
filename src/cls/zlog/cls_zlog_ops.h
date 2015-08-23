#ifndef CLS_ZLOG_OPS_H
#define CLS_ZLOG_OPS_H

#include "cls_zlog_client.h"

using namespace zlog;

struct cls_zlog_seal_op {
  uint64_t epoch;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(epoch, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(epoch, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_zlog_seal_op)

struct cls_zlog_fill_op {
  uint64_t epoch;
  uint64_t position;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(epoch, bl);
    ::encode(position, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(epoch, bl);
    ::decode(position, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_zlog_fill_op)

struct cls_zlog_write_op {
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
WRITE_CLASS_ENCODER(cls_zlog_write_op)

struct cls_zlog_read_op {
  uint64_t epoch;
  uint64_t position;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(epoch, bl);
    ::encode(position, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(epoch, bl);
    ::decode(position, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_zlog_read_op)

struct cls_zlog_max_position_op {
  uint64_t epoch;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(epoch, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(epoch, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_zlog_max_position_op)

struct cls_zlog_max_position_ret {
  uint64_t position;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(position, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(position, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_zlog_max_position_ret)

struct cls_zlog_get_projection_ret {
  uint64_t epoch;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(epoch, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(epoch, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_zlog_get_projection_ret)

#endif
