#include "objclass/objclass.h"
#include "cls_zlog_bench_ops.h"
#include "cls_zlog_bench_client.h"

CLS_VER(1,0)
CLS_NAME(zlog_bench)

cls_handle_t h_class;
cls_method_handle_t h_append;

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
  if (ret) {
    CLS_ERR("ERROR: append: stat error: %d", ret);
    return ret;
  }

  ret = cls_cxx_write(hctx, size, op.data.length(), &op.data);
  if (ret) {
    CLS_ERR("ERROR: append: write error: %d", ret);
    return ret;
  }

  return 0;
}

void __cls_init()
{
  CLS_LOG(20, "loading cls_zlog_bench");

  cls_register("zlog_bench", &h_class);

  cls_register_cxx_method(h_class, "append",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          append, &h_append);
}
