#include "objclass/objclass.h"
#include "cls_zlog_bench_ops.h"
#include "cls_zlog_bench_client.h"

//#define ZLOG_PRINT_DEBUG

CLS_VER(1,0)
CLS_NAME(zlog_bench)

cls_handle_t h_class;

cls_method_handle_t h_append;
cls_method_handle_t h_append_init;
cls_method_handle_t h_append_omap_index;
cls_method_handle_t h_append_check_epoch;

cls_method_handle_t h_map_write_null;
cls_method_handle_t h_map_write_full;

cls_method_handle_t h_stream_write_null;
cls_method_handle_t h_stream_write_full;

#define ZLOG_EPOCH_KEY "____zlog.epoch"
#define ZLOG_POS_PREFIX "____zlog.pos."

/*
 * Convert value into zero-padded string for omap comparisons.
 */
static inline std::string u64tostr(uint64_t value)
{
  std::stringstream ss;
  ss << ZLOG_POS_PREFIX << std::setw(20) << std::setfill('0') << value;
  return ss.str();
}

/*
 * Overhead-free log entry append to stream.
 */
static int append(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  cls_zlog_bench_append_op op;
  try {
    bufferlist::iterator it = in->begin();
    ::decode(op, it);
  } catch (buffer::error& err) {
    CLS_LOG(0, "ERROR: append(): failed to decode input");
    return -EINVAL;
  }

  uint64_t size;
  int ret = cls_cxx_stat(hctx, &size, NULL);
  if (ret < 0 && ret != -ENOENT) {
    CLS_ERR("ERROR: append: stat error: %d", ret);
    return ret;
  }

  if (ret == -ENOENT)
    size = 0;

  ret = cls_cxx_write(hctx, size, op.data.length(), &op.data);
  if (ret) {
    CLS_ERR("ERROR: append: write error: %d", ret);
    return ret;
  }

#ifdef ZLOG_PRINT_DEBUG
  CLS_LOG(0, "APPEND NO INDEX: %llu %llu %llu\n",
      (unsigned long long)op.epoch,
      (unsigned long long)op.position,
      (unsigned long long)op.data.length());
#endif

  return 0;
}

static int get_epoch(cls_method_context_t hctx, uint64_t *pepoch)
{
  bufferlist bl;
  int ret = cls_cxx_map_get_val(hctx, ZLOG_EPOCH_KEY, &bl);
  if (ret < 0)
    return ret;

  uint64_t cur_epoch;
  try {
    bufferlist::iterator it = bl.begin();
    ::decode(cur_epoch, it);
  } catch (buffer::error& err) {
    CLS_LOG(0, "ERROR: get_epoch(): failed to decode epoch entry");
    return -EIO;
  }

  if (pepoch)
    *pepoch = cur_epoch;

  return 0;
}

static int check_epoch(cls_method_context_t hctx, uint64_t epoch)
{
  uint64_t cur_epoch;
  int ret = get_epoch(hctx, &cur_epoch);
  if (ret < 0) {
    if (ret == -ENOENT) {
      CLS_ERR("ERROR: check_epoch: failed to read epoch... init?");
      return ret;
    }
    CLS_ERR("ERROR: check_epoch(): failed to read epoch (%d)", ret);
    return ret;
  }
  assert(ret == 0);

  if (epoch <= cur_epoch) {
    CLS_LOG(0, "NOTICE: check_update(): old epoch proposed");
    return -EINVAL;
  }

  return 0;
}

static int append_init(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  int ret = get_epoch(hctx, NULL);
  if (ret != -ENOENT) {
    if (ret == 0) {
      CLS_ERR("ERROR: append_init: received multiple init reqs!");
      return -EIO;
    }
    CLS_ERR("ERROR: append_init: unexpected return value: %d", ret);
    return ret;
  }
  assert(ret == -ENOENT);

  uint64_t new_epoch = 10;
  bufferlist bl;
  ::encode(new_epoch, bl);

  ret = cls_cxx_map_set_val(hctx, ZLOG_EPOCH_KEY, &bl);
  if (ret < 0) {
    CLS_ERR("ERROR: check_epoch: failed to initialize object");
    return ret;
  }

#ifdef ZLOG_PRINT_DEBUG
  CLS_LOG(0, "APPEND INIT NEW EPOCH = 10");
#endif

  return 0;
}

/*
 * Stream append with epoch guard.
 */
static int append_check_epoch(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  cls_zlog_bench_append_op op;
  try {
    bufferlist::iterator it = in->begin();
    ::decode(op, it);
  } catch (buffer::error& err) {
    CLS_LOG(0, "ERROR: append(): failed to decode input");
    return -EINVAL;
  }

  int ret = check_epoch(hctx, op.epoch);
  if (ret) {
    CLS_ERR("NOTICE: append_check_epoch(): stale epoch value");
    return ret;
  }

  uint64_t size;
  ret = cls_cxx_stat(hctx, &size, NULL);
  if (ret) {
    CLS_ERR("ERROR: append_check_epoch: stat error: %d", ret);
    return ret;
  }

  ret = cls_cxx_write(hctx, size, op.data.length(), &op.data);
  if (ret) {
    CLS_ERR("ERROR: append_check_epoch: write error: %d", ret);
    return ret;
  }

#ifdef ZLOG_PRINT_DEBUG
  CLS_LOG(0, "APPEND CHECK EPOCH: %llu %llu %llu\n",
      (unsigned long long)op.epoch,
      (unsigned long long)op.position,
      (unsigned long long)op.data.length());
#endif

  return 0;
}

struct cls_zlog_log_entry_md {
  uint64_t offset;
  size_t length;
  char flags;

  cls_zlog_log_entry_md() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(offset, bl);
    ::encode(length, bl);
    ::encode(flags, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(offset, bl);
    ::decode(length, bl);
    ::decode(flags, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_zlog_log_entry_md)

/*
 * Stream append with epoch guard and omap-based indexing. In this mode we are
 * appending to the object bytestream, but putting all of the metadata like
 * entry state and logical-to-physical mapping into omap.
 */
static int append_omap_index(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  cls_zlog_bench_append_op op;
  try {
    bufferlist::iterator it = in->begin();
    ::decode(op, it);
  } catch (buffer::error& err) {
    CLS_LOG(0, "ERROR: append_omap_index: failed to decode input");
    return -EINVAL;
  }

  int ret = check_epoch(hctx, op.epoch);
  if (ret) {
    CLS_ERR("NOTICE: append_omap_index: stale epoch value");
    return ret;
  }

  // lookup position in index
  bufferlist bl;
  std::string key = u64tostr(op.position);
  ret = cls_cxx_map_get_val(hctx, key, &bl);
  if (ret < 0 && ret != -ENOENT) {
    CLS_LOG(0, "ERROR: append_omap_index: failed to read index");
    return ret;
  }

  // if position hasn't been written, we'll take it
  if (ret == -ENOENT) {

    // get the append offset
    uint64_t size;
    ret = cls_cxx_stat(hctx, &size, NULL);
    if (ret) {
      CLS_ERR("ERROR: append_check_epoch: stat error: %d", ret);
      return ret;
    }

    // prepare metadata and add to index
    cls_zlog_log_entry_md md;
    md.offset = size;
    md.length = op.data.length();

    // append the log entry data
    ret = cls_cxx_write(hctx, size, op.data.length(), &op.data);
    if (ret) {
      CLS_ERR("ERROR: append_check_epoch: write error: %d", ret);
      return ret;
    }

    bufferlist mdbl;
    ::encode(md, mdbl);
    ret = cls_cxx_map_set_val(hctx, key, &mdbl);
    if (ret < 0) {
      CLS_LOG(0, "ERROR: write(): failed to write index");
      return ret;
    }

    // update max position? nope. in zlog we would update the current max
    // position. but it should be much more efficient to spend some time
    // figuring out what hte maximum position is when we need to, which is
    // relatively infrequent.

#ifdef ZLOG_PRINT_DEBUG
  CLS_LOG(0, "APPEND OMAP INDEX: %llu %llu %llu %llu\n",
      (unsigned long long)op.epoch,
      (unsigned long long)op.position,
      (unsigned long long)md.offset,
      (unsigned long long)md.length);
#endif

    return 0;
  }

  // this would be an error related to the write failing because the log entry
  // position had already been written, filled, or trimmed.
  return -EROFS;
}

/*
 * Overhead-free omap. This is omap passthrough mode.
 */
static int map_write_null(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  cls_zlog_bench_append_op op;
  try {
    bufferlist::iterator it = in->begin();
    ::decode(op, it);
  } catch (buffer::error& err) {
    CLS_LOG(0, "ERROR: map_write_null(): failed to decode input");
    return -EINVAL;
  }

  std::string key = u64tostr(op.position);
  int ret = cls_cxx_map_set_val(hctx, key, &op.data);
  if (ret < 0) {
    CLS_ERR("ERROR: map_write_null: could not write entry: %d", ret);
    return ret;
  }

#ifdef ZLOG_PRINT_DEBUG
  CLS_LOG(0, "MAP WRITE NULL: %llu %llu %llu\n",
      (unsigned long long)op.epoch,
      (unsigned long long)op.position,
      (unsigned long long)op.data.length());
#endif

  return 0;
}

struct cls_zlog_log_entry {
  bufferlist data;
  char flags;

  cls_zlog_log_entry() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(data, bl);
    ::encode(flags, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(data, bl);
    ::decode(flags, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_zlog_log_entry)

/*
 * map/n1 full overhead version
 */
static int map_write_full(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  cls_zlog_bench_append_op op;
  try {
    bufferlist::iterator it = in->begin();
    ::decode(op, it);
  } catch (buffer::error& err) {
    CLS_LOG(0, "ERROR: map_write_full: failed to decode input");
    return -EINVAL;
  }

  int ret = check_epoch(hctx, op.epoch);
  if (ret) {
    CLS_ERR("NOTICE: map_write_full: stale epoch value");
    return ret;
  }

  // lookup position in index
  bufferlist bl;
  std::string key = u64tostr(op.position);
  ret = cls_cxx_map_get_val(hctx, key, &bl);
  if (ret < 0 && ret != -ENOENT) {
    CLS_LOG(0, "ERROR: map_write_full: failed to read index");
    return ret;
  }

  // if position hasn't been written, we'll take it
  if (ret == -ENOENT) {
    // setup entry
    cls_zlog_log_entry entry;
    entry.data = op.data;

    bufferlist entrybl;
    ::encode(entry, entrybl);
    ret = cls_cxx_map_set_val(hctx, key, &entrybl);
    if (ret < 0) {
      CLS_LOG(0, "ERROR: map_write_full(): failed to write index");
      return ret;
    }

    // update max position? nope. in zlog we would update the current max
    // position. but it should be much more efficient to spend some time
    // figuring out what hte maximum position is when we need to, which is
    // relatively infrequent.

#ifdef ZLOG_PRINT_DEBUG
  CLS_LOG(0, "MAP WRITE FULL: %llu %llu %llu\n",
      (unsigned long long)op.epoch,
      (unsigned long long)op.position,
      (unsigned long long)op.data.length());
#endif

    return 0;
  }

  // this would be an error related to the write failing because the log entry
  // position had already been written, filled, or trimmed.
  return -EROFS;
}

/*
 * Overhead-free stream write.
 */
static int stream_write_null(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  cls_zlog_bench_append_op op;
  try {
    bufferlist::iterator it = in->begin();
    ::decode(op, it);
  } catch (buffer::error& err) {
    CLS_LOG(0, "ERROR: stream write null: failed to decode input");
    return -EINVAL;
  }

  /*
   * A design would either communicate the logical position, or the offset of
   * the write (probably the later). Here we avoid creating an entirely new
   * data structure and just re-use op.position for the write offset.
   */
  int ret = cls_cxx_write(hctx, op.position, op.data.length(), &op.data);
  if (ret) {
    CLS_ERR("ERROR: stream write null: write error: %d", ret);
    return ret;
  }

#ifdef ZLOG_PRINT_DEBUG
  CLS_LOG(0, "STREAM WRITE NULL: %llu %llu %llu\n",
      (unsigned long long)op.epoch,
      (unsigned long long)op.position,
      (unsigned long long)op.data.length());
#endif

  return 0;
}

struct cls_zlog_log_entry_small_md {
  char flags;

  cls_zlog_log_entry_small_md() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(flags, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(flags, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_zlog_log_entry_small_md)

/*
 * Stream write with explicit offset
 */
static int stream_write_full(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  cls_zlog_bench_append_op op;
  try {
    bufferlist::iterator it = in->begin();
    ::decode(op, it);
  } catch (buffer::error& err) {
    CLS_LOG(0, "ERROR: stream write full: failed to decode input");
    return -EINVAL;
  }

  int ret = check_epoch(hctx, op.epoch);
  if (ret) {
    CLS_ERR("NOTICE: stream write full: stale epoch value");
    return ret;
  }

  /*
   * in this case we are keeping the index in terms of the phyiscal offset
   * rather than the logical entry position.
   */
  bufferlist bl;
  std::string key = u64tostr(op.position);
  ret = cls_cxx_map_get_val(hctx, key, &bl);
  if (ret < 0 && ret != -ENOENT) {
    CLS_LOG(0, "ERROR: stream write full: failed to read index");
    return ret;
  }

  // if position hasn't been written, we'll take it
  if (ret == -ENOENT) {

    // append the log entry data
    ret = cls_cxx_write(hctx, op.position, op.data.length(), &op.data);
    if (ret) {
      CLS_ERR("ERROR: stream write full: write error: %d", ret);
      return ret;
    }

    // metadata is just about the entry state... no logical to physical
    // mapping metadata is necessary
    cls_zlog_log_entry_small_md md;
    bufferlist mdbl;
    ::encode(md, mdbl);
    ret = cls_cxx_map_set_val(hctx, key, &mdbl);
    if (ret < 0) {
      CLS_LOG(0, "ERROR: stream write full: failed to write index");
      return ret;
    }

    // update max position? nope. in zlog we would update the current max
    // position. but it should be much more efficient to spend some time
    // figuring out what hte maximum position is when we need to, which is
    // relatively infrequent.

#ifdef ZLOG_PRINT_DEBUG
  CLS_LOG(0, "STREAM WRITE FULL: %llu %llu %llu\n",
      (unsigned long long)op.epoch,
      (unsigned long long)op.position,
      (unsigned long long)op.data.length());
#endif

    return 0;
  }

  // this would be an error related to the write failing because the log entry
  // position had already been written, filled, or trimmed.
  return -EROFS;
}

void __cls_init()
{
  CLS_LOG(20, "loading cls_zlog_bench");

  cls_register("zlog_bench", &h_class);

  cls_register_cxx_method(h_class, "append",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          append, &h_append);

  cls_register_cxx_method(h_class, "append_check_epoch",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          append_check_epoch, &h_append_check_epoch);

  cls_register_cxx_method(h_class, "append_omap_index",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          append_omap_index, &h_append_omap_index);

  cls_register_cxx_method(h_class, "append_init",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          append_init, &h_append_init);

  cls_register_cxx_method(h_class, "map_write_null",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          map_write_null, &h_map_write_null);

  cls_register_cxx_method(h_class, "map_write_full",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          map_write_full, &h_map_write_full);

  cls_register_cxx_method(h_class, "stream_write_null",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          stream_write_null, &h_stream_write_null);

  cls_register_cxx_method(h_class, "stream_write_full",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          stream_write_full, &h_stream_write_full);
}
