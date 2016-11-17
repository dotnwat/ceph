#ifndef CLS_PHYDESIGN_H
#define CLS_PHYDESIGN_H

#define INIT_STATE              1

#define READ_EPOCH_OMAP         2
#define READ_EPOCH_OMAP_HDR     3
#define READ_EPOCH_XATTR        4
#define READ_EPOCH_NOOP         5
#define READ_EPOCH_HEADER       6

#define READ_OMAP_INDEX_ENTRY   7
#define WRITE_OMAP_INDEX_ENTRY  8

#define APPEND_DATA             9

struct entry_info {
  uint64_t position;
  ceph::bufferlist data;

  entry_info() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(position, bl);
    ::encode(data, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(position, bl);
    ::decode(data, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(entry_info)

#endif
