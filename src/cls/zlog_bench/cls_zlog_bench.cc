#include "objclass/objclass.h"
#include "cls_zlog_bench_ops.h"
#include "cls_zlog_bench_client.h"

#define ZLOG_PRINT_DEBUG

CLS_VER(1,0)
CLS_NAME(zlog_bench)

cls_handle_t h_class;
cls_method_handle_t h_append;
cls_method_handle_t h_append_init;
cls_method_handle_t h_append_check_epoch;

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
      op.epoch,
      op.position,
      op.data.length());
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
      op.epoch,
      op.position,
      op.data.length());
#endif

  return 0;
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

  cls_register_cxx_method(h_class, "append_init",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          append_init, &h_append_init);
}
