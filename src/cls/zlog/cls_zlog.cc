#include <errno.h>
#include <boost/lexical_cast.hpp>
#include "objclass/objclass.h"
#include "cls_zlog_ops.h"
#include "cls_zlog_client.h"

/*
 * Improvement:
 *   - Should max pos be updated with each write, or just calculate it when
 *   asked?
 */

CLS_VER(1,0)
CLS_NAME(zlog)

cls_handle_t h_class;
cls_method_handle_t h_seal;
cls_method_handle_t h_fill;
cls_method_handle_t h_write;
cls_method_handle_t h_read;
cls_method_handle_t h_max_position;

cls_method_handle_t h_set_projection;
cls_method_handle_t h_get_projection;

#define ZLOG_EPOCH_KEY "____zlog.epoch"
#define ZLOG_PROJECTION_KEY  "____zlog.projection"
#define ZLOG_POS_PREFIX "____zlog.pos."
#define ZLOG_MAX_POS_KEY "____zlog.max_position"

struct cls_zlog_log_entry {
  int flags;
  bufferlist data;

  static const int INVALIDATED = 1;

  cls_zlog_log_entry() : flags(0)
  {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(flags, bl);
    ::encode(data, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(flags, bl);
    ::decode(data, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_zlog_log_entry)

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
 * Convert string into numeric value.
 */
static inline int strtou64(const std::string value, uint64_t *out)
{
  uint64_t v;

  //assert expected prefix

  try {
    std::string value2 = value.substr(strlen(ZLOG_POS_PREFIX));
    v = boost::lexical_cast<uint64_t>(value2);
  } catch (boost::bad_lexical_cast &) {
    CLS_ERR("converting key into numeric value %s", value.c_str());
    return -EIO;
  }
 
  *out = v;
  return 0;
}

static int check_epoch(cls_method_context_t hctx, uint64_t epoch)
{
  bufferlist bl;
  int ret = cls_cxx_map_get_val(hctx, ZLOG_EPOCH_KEY, &bl);
  if (ret < 0) {
    CLS_LOG(10, "ERROR: check_epoch(): failed to read epoch (%d)", ret);
    return ret;
  }

  uint64_t cur_epoch;
  try {
    bufferlist::iterator it = bl.begin();
    ::decode(cur_epoch, it);
  } catch (buffer::error& err) {
    CLS_LOG(0, "ERROR: check_epoch(): failed to decode epoch entry");
    return -EIO;
  }

  if (epoch < cur_epoch) {
    CLS_LOG(0, "NOTICE: check_update(): old epoch proposed");
    return CLS_ZLOG_STALE_EPOCH;
  }

  return 0;
}

static int __max_position(cls_method_context_t hctx, uint64_t *pposition)
{
  // read max_position from omap
  bufferlist bl;
  int ret = cls_cxx_map_get_val(hctx, ZLOG_MAX_POS_KEY, &bl);
  if (ret < 0) {
    CLS_LOG(10, "NOTICE: __max_position(): failed to read max_position (%d)", ret);
    return ret;
  }

  // decode
  uint64_t position;
  try {
    bufferlist::iterator it = bl.begin();
    ::decode(position, it);
  } catch (buffer::error& err) {
    CLS_LOG(0, "ERROR: __max_position(): failed to decode max_position entry");
    return -EIO;
  }

  *pposition = position;
  return 0;
}

static int read(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  // decode input operation
  cls_zlog_read_op op;
  try {
    bufferlist::iterator it = in->begin();
    ::decode(op, it);
  } catch (buffer::error& err) {
    CLS_LOG(0, "ERROR: read(): failed to decode input");
    return -EINVAL;
  }

  int ret = check_epoch(hctx, op.epoch);
  if (ret) {
    CLS_LOG(10, "NOTICE: read(): stale epoch value");
    return ret;
  }

  // lookup position in omap index
  bufferlist bl;
  std::string key = u64tostr(op.position);
  ret = cls_cxx_map_get_val(hctx, key, &bl);
  if (ret < 0 && ret != -ENOENT) {
    CLS_LOG(0, "ERROR: read(): failed to read from index");
    return ret;
  }

  // if not found, return NOT_WRITTEN status
  if (ret == -ENOENT)
    return CLS_ZLOG_NOT_WRITTEN;

  // otherwise try to decode the entry
  cls_zlog_log_entry entry;
  try {
    bufferlist::iterator it = bl.begin();
    ::decode(entry, it);
  } catch (buffer::error& err) {
    CLS_LOG(0, "ERROR: read(): failed to decode index entry");
    return -EIO;
  }

  // the entry might have been filled
  if (entry.flags & cls_zlog_log_entry::INVALIDATED)
    return CLS_ZLOG_INVALIDATED;

  *out = entry.data;

  return CLS_ZLOG_OK;
}

static int write(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  // decode input operation
  cls_zlog_write_op op;
  try {
    bufferlist::iterator it = in->begin();
    ::decode(op, it);
  } catch (buffer::error& err) {
    CLS_LOG(0, "ERROR: write(): failed to decode input");
    return -EINVAL;
  }

  int ret = check_epoch(hctx, op.epoch);
  if (ret) {
    CLS_LOG(10, "NOTICE: write(): stale epoch value");
    return ret;
  }

  // lookup position in index
  bufferlist bl;
  std::string key = u64tostr(op.position);
  ret = cls_cxx_map_get_val(hctx, key, &bl);
  if (ret < 0 && ret != -ENOENT) {
    CLS_LOG(0, "ERROR: write(): failed to read index");
    return ret;
  }

  // if position hasn't been written, we'll take it!
  if (ret == -ENOENT) {
    cls_zlog_log_entry entry;
    entry.data = op.data;

    bufferlist entrybl;
    ::encode(entry, entrybl);
    ret = cls_cxx_map_set_val(hctx, key, &entrybl);
    if (ret < 0) {
      CLS_LOG(0, "ERROR: write(): failed to write index");
      return ret;
    }

    uint64_t cur_max_position = 0;
    ret = __max_position(hctx, &cur_max_position);
    if (ret < 0 && ret != -ENOENT)
      return ret;

    if (ret != -ENOENT)
      assert(op.position != cur_max_position);

    // update the max_position? the first test will always update the max_position to the
    // maximum written position. the second test will be true if the max_position
    // hasn't yet been set. note that if the first write is to position 0,
    // then the second condition lets the max_position initialization occur.
    if (op.position > cur_max_position || ret == -ENOENT) {
      bufferlist max_positionbl;
      ::encode(op.position, max_positionbl);
      ret = cls_cxx_map_set_val(hctx, ZLOG_MAX_POS_KEY, &max_positionbl);
      if (ret < 0) {
        CLS_LOG(0, "ERROR: write(): failed to update max_position");
        return ret;
      }
    }

    return CLS_ZLOG_OK;
  }

  return CLS_ZLOG_READ_ONLY;
}

static int fill(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  // decode input operation
  cls_zlog_fill_op op;
  try {
    bufferlist::iterator it = in->begin();
    ::decode(op, it);
  } catch (buffer::error& err) {
    CLS_LOG(0, "ERROR: fill(): failed to decode input");
    return -EINVAL;
  }

  int ret = check_epoch(hctx, op.epoch);
  if (ret) {
    CLS_LOG(10, "NOTICE: fill(): stale epoch value");
    return ret;
  }

  // lookup position in the omap index
  bufferlist bl;
  std::string key = u64tostr(op.position);
  ret = cls_cxx_map_get_val(hctx, key, &bl);
  if (ret < 0 && ret != -ENOENT) {
    CLS_LOG(0, "ERROR: fill(): failed to read index");
    return ret;
  }

  cls_zlog_log_entry entry;

  // if position hasn't been written, invalidate it
  if (ret == -ENOENT) {
    entry.flags |= cls_zlog_log_entry::INVALIDATED;
    bufferlist entrybl;
    ::encode(entry, entrybl);
    ret = cls_cxx_map_set_val(hctx, key, &entrybl);
    if (ret < 0) {
      CLS_LOG(0, "ERROR: fill(): failed to write index");
      return ret;
    }
    return CLS_ZLOG_OK;
  }

  // decode the entry from the index
  try {
    bufferlist::iterator it = bl.begin();
    ::decode(entry, it);
  } catch (buffer::error& err) {
    CLS_LOG(0, "ERROR: fill(): failed to decode log entry");
    return -EIO;
  }

  // if it is already invalidated, then report success
  if (entry.flags & cls_zlog_log_entry::INVALIDATED)
    return CLS_ZLOG_OK;

  return CLS_ZLOG_READ_ONLY;
}

static int seal(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  // decode input operation
  cls_zlog_seal_op op;
  try {
    bufferlist::iterator it = in->begin();
    ::decode(op, it);
  } catch (buffer::error& err) {
    CLS_LOG(0, "ERROR: seal(): failed to decode input");
    return -EINVAL;
  }

  // read the current epoch value (may not yet be set)
  bufferlist bl;
  int ret = cls_cxx_map_get_val(hctx, ZLOG_EPOCH_KEY, &bl);
  if (ret < 0 && ret != -ENOENT) {
    CLS_LOG(10, "NOTICE: seal(): failed to read max_position (%d)", ret);
    return ret;
  }

  // if an epoch exists, verify that the new epoch is larger
  if (ret != -ENOENT) {
    uint64_t cur_epoch;

    try {
      bufferlist::iterator it = bl.begin();
      ::decode(cur_epoch, it);
    } catch (buffer::error& err) {
      CLS_LOG(0, "ERROR: seal(): failed to decode epoch entry");
      return -EIO;
    }

    if (op.epoch <= cur_epoch) {
      CLS_LOG(0, "ERROR: seal(): epochs move strictly forward");
      return CLS_ZLOG_INVALID_EPOCH;
    }
  }

  // set new epoch value in omap
  bufferlist epochbl;
  ::encode(op.epoch, epochbl);
  ret = cls_cxx_map_set_val(hctx, ZLOG_EPOCH_KEY, &epochbl);
  if (ret < 0) {
    CLS_LOG(0, "ERROR: seal(): failed to update epoch");
    return ret;
  }

  return CLS_ZLOG_OK;
}

static int max_position(cls_method_context_t hctx, bufferlist *in,
    bufferlist *out)
{
  cls_zlog_max_position_op op;
  try {
    bufferlist::iterator it = in->begin();
    ::decode(op, it);
  } catch (buffer::error& err) {
    CLS_LOG(0, "ERROR: max_position(): failed to decode input");
    return -EINVAL;
  }

  int ret = check_epoch(hctx, op.epoch);
  if (ret) {
    CLS_LOG(10, "NOTICE: max_position): stale epoch value");
    return ret;
  }

  uint64_t position;
  ret = __max_position(hctx, &position);
  if (ret < 0)
    return ret;

  cls_zlog_max_position_ret reply;
  reply.position = position;

  ::encode(reply, *out);

  return CLS_ZLOG_OK;
}

static int __get_projection(cls_method_context_t hctx, uint64_t *pepoch)
{
  bufferlist bl;
  int ret = cls_cxx_map_get_val(hctx, ZLOG_PROJECTION_KEY, &bl);
  if (ret < 0)
    return ret;

  uint64_t epoch;
  try {
    bufferlist::iterator it = bl.begin();
    ::decode(epoch, it);
  } catch (buffer::error& err) {
    CLS_LOG(0, "ERROR: set_projection(): failed to decode projection");
    return -EIO;
  }

  *pepoch = epoch;

  return 0;
}

static int __set_projection(cls_method_context_t hctx, uint64_t epoch)
{
  bufferlist bl;
  ::encode(epoch, bl);

  int ret = cls_cxx_map_set_val(hctx, ZLOG_PROJECTION_KEY, &bl);
  if (ret < 0) {
    CLS_LOG(0, "ERROR: __set_projction(): could not set projection epoch");
    return ret;
  }

  return 0;
}

static int set_projection(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  uint64_t epoch;
  int ret = __get_projection(hctx, &epoch);
  if (ret < 0 && ret != -ENOENT) {
    CLS_LOG(0, "ERROR: failed to read projection");
    return ret;
  }

  if (ret == -ENOENT)
    epoch = 0;
  else
    epoch++;

  ret = __set_projection(hctx, epoch);
  if (ret < 0) {
    CLS_LOG(0, "ERROR: set_projection(): failed to set projection");
    return ret;
  }

  return 0;
}

static int get_projection(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  uint64_t epoch;
  int ret = __get_projection(hctx, &epoch);
  if (ret < 0) {
    return ret;
  }

  cls_zlog_get_projection_ret reply;
  reply.epoch = epoch;
  ::encode(reply, *out);

  return 0;
}

void __cls_init()
{
  CLS_LOG(0, "loading cls_zlog");

  cls_register("zlog", &h_class);

  cls_register_cxx_method(h_class, "seal",
      CLS_METHOD_RD | CLS_METHOD_WR,
      seal, &h_seal);

  cls_register_cxx_method(h_class, "fill",
			  CLS_METHOD_RD | CLS_METHOD_WR,
			  fill, &h_fill);

  cls_register_cxx_method(h_class, "write",
			  CLS_METHOD_RD | CLS_METHOD_WR,
			  write, &h_write);

  cls_register_cxx_method(h_class, "read",
			  CLS_METHOD_RD,
			  read, &h_read);

  cls_register_cxx_method(h_class, "max_position",
			  CLS_METHOD_RD,
			  max_position, &h_max_position);

  cls_register_cxx_method(h_class, "get_projection",
			  CLS_METHOD_RD,
			  get_projection, &h_get_projection);

  cls_register_cxx_method(h_class, "set_projection",
			  CLS_METHOD_RD | CLS_METHOD_WR,
			  set_projection, &h_set_projection);
}
