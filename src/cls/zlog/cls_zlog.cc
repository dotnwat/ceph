#include <errno.h>
#include <boost/lexical_cast.hpp>
#include "objclass/objclass.h"
#include "cls_zlog_ops.h"
#include "cls_zlog_client.h"

/*
 * FIXME:
 *  - check that epoch guard treats the current epoch as _sealed_ meaning that
 *  operations tagged with the current epoch are rejected.
 *
 * Improvement:
 *   - Should max pos be updated with each write, or just calculate it when
 *   asked?
 *   - Trimming should use a watermark to mark log positions below which all
 *   positions are trimmed so that entries that only contain a set trim bit
 *   can be released from the index.
 *
 * write: write is only used for log append, and when a write fails because
 * the entry is read-only then the append is repeated with a new position from
 * the sequencer. written entries are explictly read-only, but junk and trim
 * positions are also treated as read-only for the purpose of informing the
 * client library to retry (the client never sees the read-only state during
 * append). in the future it may be desirable to return written, trimmed, or
 * junk instead of read-only. writing to a trimmed or junk filled position are
 * both posibilities during race conditions.
 *
 * write, empty:    ok
 * write, written:  read-only
 * write, trimmed:  read-only
 * write, filled:   read-only
 *
 * read: two use cases for read are important to distinguish. a client may be
 * replaying a log and encounter a junk entry which would be skipped. an entry
 * may be trimmed, too. a scanning client would skip this. however, a client
 * should be able to see some specific state information. for instance, a
 * client that uses a position as a pointer that should always be valid may
 * want to know when a pointer is invalid (e.g. an assertion error).
 *
 * when an entry is trimmed a read will return the trimmed error. filled could
 * return the invalidated error, but we also want to immediately trim filled
 * locations. finally, when a range of entries is trimmed they all return
 * trimmed meaning that entries that were filled can't have individual state
 * tracked. so, filled should also return trimmed.
 *
 * NOTE: currently invalidated is returend for both trimmed and filled
 * positions on read. per the discussion above, trimmed should be returned in
 * both cases. for now we re-use the invalidated flag.
 *
 * read, empty:    not-written
 * read, written:  ok
 * read, trimmed:  invalidated (should be trimmed)
 * read, filled:   invalidated (should be trimmed)
 *
 * fill: fill is idempotent when it is already filled. the affect of filling a
 * trimmed position is also treated as a success because the overall effect is
 * the same---the position cannot be read. however, if applications want the
 * extra state information in the future (e.g. filling trimmed returns trim
 * error) then we can add that.
 *
 * fill, empty:    ok
 * fill, written:  read-only
 * fill, trimmed:  ok
 * fill, filled:   ok
 *
 * trim: trimming is like filling, but also lets users gc a position that has
 * been written. we allow trim to always succeed, but it is debatable if trim
 * should succeed for an empty position (since it is already unused).
 *
 * trim, empty:   ok
 * trim, written: ok
 * trim, trimmed: ok
 * trim, filled:  ok
 */

CLS_VER(1,0)
CLS_NAME(zlog)

cls_handle_t h_class;
cls_method_handle_t h_seal;
cls_method_handle_t h_fill;
cls_method_handle_t h_write;
cls_method_handle_t h_read;
cls_method_handle_t h_trim;
cls_method_handle_t h_max_position;

cls_method_handle_t h_set_projection;
cls_method_handle_t h_get_projection;

#define ZLOG_EPOCH_KEY "____zlog.epoch"
#define ZLOG_POS_PREFIX "____zlog.pos."
#define ZLOG_MAX_POS_KEY "____zlog.max_position"

// per-epoch key prefix
#define ZLOG_PROJECTION_PREFIX  "____zlog.projection."
// maximum epoch tracked
#define ZLOG_LATEST_PROJECTION_KEY "____zlog.latest_projection"

struct cls_zlog_log_entry {
  int flags;
  bufferlist data;

  static const int INVALIDATED = 1;
  static const int TRIMMED     = 2;

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
    CLS_LOG(0, "ERROR: check_epoch(): failed to decode epoch entry");
    return -EIO;
  }

  *pepoch = cur_epoch;

  return 0;
}

static int check_epoch(cls_method_context_t hctx, uint64_t epoch)
{
  uint64_t cur_epoch;
  int ret = get_epoch(hctx, &cur_epoch);
  if (ret < 0 && ret != -ENOENT) {
    CLS_LOG(10, "ERROR: check_epoch(): failed to read epoch (%d)", ret);
    return ret;
  }

  /*
   * Handle initialization case. Effectively this simulates an initial sealed
   * epoch of -1, which is always less than the epoch of an operation which is
   * unsigned. This allows us to avoid the ugly case where initially we create
   * epoch 0 and epoch 1, then immediately seal epoch 0.
   *
   * Note that this implies that even though the target object may not exist,
   * and ZLOG_EPOCH_KEY may not be set, all unit tests must pass as if the
   * real sealed epoch is -1 and that should be taken into account in the unit
   * tests.
   */
  if (ret == -ENOENT) {
    CLS_LOG(0, "NOTICE: treating non-init object as cur_epoch = -1");
    return 0;
  }

  if (epoch <= cur_epoch) {
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

  // the entry might have been filled or invalidated.
  if (entry.flags & cls_zlog_log_entry::INVALIDATED ||
      entry.flags & cls_zlog_log_entry::TRIMMED)
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

  /*
   * currently both junk and trimmed entries are kept in the index so this
   * works. with optimization in which ranges of trimmed entries are replaced
   * by a small piece of metadata will require changes here because read-only
   * is returned even though the entry won't be in the index, and hence ret ==
   * -ENOENT as above.
   */
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
    entry.flags |= cls_zlog_log_entry::TRIMMED;
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

  // if it is already invalidated or filled, then report success
  if (entry.flags & cls_zlog_log_entry::INVALIDATED ||
      entry.flags & cls_zlog_log_entry::TRIMMED)
    return CLS_ZLOG_OK;

  return CLS_ZLOG_READ_ONLY;
}

static int trim(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  // decode input operation
  cls_zlog_trim_op op;
  try {
    bufferlist::iterator it = in->begin();
    ::decode(op, it);
  } catch (buffer::error& err) {
    CLS_LOG(0, "ERROR: trim(): failed to decode input");
    return -EINVAL;
  }

  int ret = check_epoch(hctx, op.epoch);
  if (ret) {
    CLS_LOG(10, "NOTICE: trim(): stale epoch value");
    return ret;
  }

  // lookup position in the omap index
  bufferlist bl;
  std::string key = u64tostr(op.position);
  ret = cls_cxx_map_get_val(hctx, key, &bl);
  if (ret < 0 && ret != -ENOENT) {
    CLS_LOG(0, "ERROR: trim(): failed to read index");
    return ret;
  }

  cls_zlog_log_entry entry;

  // if position hasn't been written, trim it
  if (ret == -ENOENT) {
    entry.flags |= cls_zlog_log_entry::TRIMMED;
    bufferlist entrybl;
    ::encode(entry, entrybl);
    ret = cls_cxx_map_set_val(hctx, key, &entrybl);
    if (ret < 0) {
      CLS_LOG(0, "ERROR: trim(): failed to write index");
      return ret;
    }
    return CLS_ZLOG_OK;
  }

  // decode the entry from the index
  try {
    bufferlist::iterator it = bl.begin();
    ::decode(entry, it);
  } catch (buffer::error& err) {
    CLS_LOG(0, "ERROR: trim(): failed to decode log entry");
    return -EIO;
  }

  if (entry.flags & cls_zlog_log_entry::TRIMMED)
    return CLS_ZLOG_OK;

  // if it exists then set the trim flag and delete the payload.
  entry.data.clear();
  entry.flags |= cls_zlog_log_entry::TRIMMED;

  bufferlist entrybl;
  ::encode(entry, entrybl);
  ret = cls_cxx_map_set_val(hctx, key, &entrybl);
  if (ret < 0) {
    CLS_LOG(0, "ERROR: trim(): failed to update index");
    return ret;
  }

  return CLS_ZLOG_OK;
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

/*
 * Ideally seal() would return the maximum position written to, but rados
 * object classes currently prevent this because data can't be returned after
 * an object update. Instead we enforce epoch equality, and require clients
 * that seal() to make another pass using the same epoch.
 *
 * The exact semantics are that this method returns the next position in the
 * log assuming a single object is being striped across. This is useful
 * because 0 indicates no writes and we don't need an extra flag to describe
 * the state of the object being empty.
 */
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

  uint64_t cur_epoch;
  int ret = get_epoch(hctx, &cur_epoch);
  if (ret < 0) {
    CLS_LOG(0, "ERROR: max_position(): could not retrieve epoch");
    return ret;
  }

  if (op.epoch != cur_epoch) {
    CLS_LOG(0, "ERROR: max_position(): invalid epoch");
    return -EINVAL;
  }

  uint64_t position;
  ret = __max_position(hctx, &position);
  if (ret < 0 && ret != -ENOENT)
    return ret;

  cls_zlog_max_position_ret reply;
  if (ret == -ENOENT)
    reply.position = 0;
  else
    reply.position = position + 1;

  ::encode(reply, *out);

  return CLS_ZLOG_OK;
}

static int __set_latest_projection(cls_method_context_t hctx,
    uint64_t epoch)
{
  bufferlist bl;
  ::encode(epoch, bl);

  int ret = cls_cxx_map_set_val(hctx, ZLOG_LATEST_PROJECTION_KEY, &bl);
  if (ret < 0) {
    CLS_LOG(0, "ERROR: could not set latest projection %llu",
        (unsigned long long)epoch);
    return ret;
  }

  return 0;
}

static int __get_latest_projection(cls_method_context_t hctx,
    uint64_t *pepoch)
{
  bufferlist bl;
  int ret = cls_cxx_map_get_val(hctx, ZLOG_LATEST_PROJECTION_KEY, &bl);
  if (ret < 0) {
    CLS_LOG(0, "__get_latest_projection: failed to get map val");
    return ret;
  }

  uint64_t epoch;
  try {
    bufferlist::iterator it = bl.begin();
    ::decode(epoch, it);
  } catch (buffer::error& err) {
    CLS_LOG(0, "ERROR: __get_latest_projection: cannot decode");
    return -EIO;
  }

  *pepoch = epoch;

  return 0;
}

static int __get_projection(cls_method_context_t hctx, bufferlist *out,
    uint64_t epoch)
{
  // build projection key: prefix.epoch
  stringstream key;
  key << ZLOG_PROJECTION_PREFIX << epoch;

  // read key from omap
  bufferlist bl;
  int ret = cls_cxx_map_get_val(hctx, key.str(), &bl);
  if (ret < 0)
    return ret;

  if (out)
    *out = bl;

  return 0;
}

static int __set_projection(cls_method_context_t hctx, uint64_t epoch, bufferlist *bl)
{
  // projections are write-once. we return if a projection already exists at
  // this epoch, or there is an error other than -ENOENT.
  int ret = __get_projection(hctx, NULL, epoch);
  if (ret == 0) {
    CLS_LOG(0, "ERROR: __set_projection: projection already set %llu",
        (unsigned long long)epoch);
    return -EINVAL;
  } else if (ret != -ENOENT) {
    CLS_LOG(0, "ERROR: __set_projection: could not get projection %llu",
        (unsigned long long)epoch);
    return ret;
  }
  assert(ret == -ENOENT);

  // build projection key: prefix.epoch
  stringstream key;
  key << ZLOG_PROJECTION_PREFIX << epoch;

  // set the projection
  ret = cls_cxx_map_set_val(hctx, key.str(), bl);
  if (ret < 0) {
    CLS_LOG(0, "ERROR: __set_projction(): could not set projection");
    return ret;
  }

  return 0;
}

static int set_projection(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  cls_zlog_set_projection_op op;
  try {
    bufferlist::iterator it = in->begin();
    ::decode(op, it);
  } catch (buffer::error& err) {
    CLS_LOG(0, "ERROR: set_projection: failed to decode input");
    return -EINVAL;
  }

  // get the latest projection, if one exists
  uint64_t epoch;
  int ret = __get_latest_projection(hctx, &epoch);
  if (ret && ret != -ENOENT) {
    CLS_LOG(0, "ERROR: set_projection: error finding latest epoch %d", ret);
    return ret;
  }

  // if not projection exists, then the first should be zero
  if (ret == -ENOENT && op.epoch != 0) {
    CLS_LOG(0, "ERROR: set_projection: first epoch must be zero %llu given",
        (unsigned long long)epoch);
    return -EINVAL;
  }

  // if a projection does exist, then we should be setting curr_proj + 1
  if (ret != -ENOENT && op.epoch != (epoch + 1)) {
    CLS_LOG(0, "ERROR: set_projection: new epoch must be curr+1 curr " \
        "%llu given %llu", (unsigned long long)op.epoch,
        (unsigned long long)epoch);
    return -EINVAL;
  }

  ret = __set_projection(hctx, op.epoch, &op.data);
  if (ret < 0) {
    CLS_LOG(0, "ERROR: set_projection(): failed to set projection");
    return ret;
  }

  ret = __set_latest_projection(hctx, op.epoch);
  if (ret < 0) {
    CLS_LOG(0, "ERROR: set_projection(): failed to set latest projection");
    return ret;
  }

  return 0;
}

static int get_projection(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  cls_zlog_get_projection_op op;
  try {
    bufferlist::iterator it = in->begin();
    ::decode(op, it);
  } catch (buffer::error& err) {
    CLS_LOG(0, "ERROR: get_projection: failed to decode input");
    return -EINVAL;
  }

  // use specified epoch, unless latest is requested
  uint64_t epoch = op.epoch;
  if (op.latest) {
    int ret = __get_latest_projection(hctx, &epoch);
    if (ret) {
      CLS_LOG(0, "ERROR: get_projection: error finding latest epoch %d", ret);
      return ret;
    }
  }

  // read the projection blob at the given epoch
  bufferlist bl;
  int ret = __get_projection(hctx, &bl, epoch);
  if (ret < 0) {
    CLS_LOG(0, "ERROR: get_projection: could not find epoch %llu ret %d",
        (unsigned long long)epoch, ret);
    return ret;
  }

  cls_zlog_get_projection_ret reply;
  reply.epoch = epoch;
  reply.out = bl;

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
			  CLS_METHOD_RD | CLS_METHOD_WR,
			  read, &h_read);

  cls_register_cxx_method(h_class, "trim",
			  CLS_METHOD_RD | CLS_METHOD_WR,
			  trim, &h_trim);

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
