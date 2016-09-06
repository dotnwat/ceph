/*
 * This is an example RADOS object class built using only the SDK interface.
 */
#include "include/rados/objclass.h"

CLS_VER(1,0)
CLS_NAME(sdk)

cls_handle_t h_class;
cls_method_handle_t h_test_coverage;

static int test_coverage(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  int ret = cls_cxx_create(hctx, false);
  if (ret < 0)
    return ret;

  uint64_t size;
  ret = cls_cxx_stat(hctx, &size, NULL);
  if (ret < 0)
    return ret;

  const char c = (char)0;
  bufferlist bl;
  bl.append(&c, sizeof(c));

  ret = cls_cxx_write(hctx, 0, bl.length(), &bl);
  if (ret < 0)
    return ret;

  bl.clear();
  ret = cls_cxx_read(hctx, 0, sizeof(c), &bl);
  if (ret < 0)
    return ret;
  if (bl.length() != sizeof(c))
    return -EIO;

  ret = cls_cxx_setxattr(hctx, "foo", &bl);
  if (ret < 0)
    return ret;

  bl.clear();
  ret = cls_cxx_getxattr(hctx, "foo", &bl);
  if (ret < 0)
    return ret;
  if (bl.length() != sizeof(c))
    return -EIO;

  ret = cls_cxx_map_set_val(hctx, "foo", &bl);
  if (ret < 0)
    return ret;

  bl.clear();
  ret = cls_cxx_map_get_val(hctx, "foo", &bl);
  if (ret < 0)
    return ret;
  if (bl.length() != sizeof(c))
    return -EIO;

  ret = cls_cxx_remove(hctx);
  if (ret < 0)
    return ret;

  return 0;
}

void __cls_init()
{
  CLS_LOG(0, "loading cls_sdk");

  cls_register("sdk", &h_class);

  cls_register_cxx_method(h_class, "test_coverage",
      CLS_METHOD_RD | CLS_METHOD_RD | CLS_METHOD_PROMOTE,
      test_coverage, &h_test_coverage);
}
