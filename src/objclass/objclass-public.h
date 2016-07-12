#ifndef CEPH_OBJCLASS_OBJCLASS_PUBLIC_H
#define CEPH_OBJCLASS_OBJCLASS_PUBLIC_H

#ifdef __cplusplus

#include "buffer.h"
extern "C" {
#endif

#define CLS_VER(maj,min) \
int __cls_ver__## maj ## _ ##min = 0; \
int __cls_ver_maj = maj; \
int __cls_ver_min = min;

#define CLS_NAME(name) \
int __cls_name__## name = 0; \
const char *__cls_name = #name;

#define CLS_METHOD_RD       0x1 /// method executes read operations
#define CLS_METHOD_WR       0x2 /// method executes write operations
#define CLS_METHOD_PROMOTE  0x8 /// method cannot be proxied to base tier

#define CLS_LOG(level, fmt, ...)                                        \
  cls_log(level, "<cls> %s:%d: " fmt, __FILE__, __LINE__, ##__VA_ARGS__)

void __cls_init();

typedef void* cls_handle_t;
typedef void* cls_method_handle_t;
typedef void* cls_method_context_t;

extern int cls_log(int level, const char *format, ...)
  __attribute__((__format__(printf, 2, 3)));

extern int cls_register(const char *name, cls_handle_t *handle);
#ifdef __cplusplus
}

typedef int (*cls_method_cxx_call_t)(cls_method_context_t ctx,
    class buffer::list *inbl, class buffer::list *outbl);

//extern int cls_get_request_origin(cls_method_context_t hctx,
//                                  entity_inst_t *origin);
extern int cls_cxx_write_full(cls_method_context_t hctx, bufferlist *bl);
extern int cls_cxx_read(cls_method_context_t hctx, int ofs, int len, bufferlist *bl);
extern int cls_cxx_setxattr(cls_method_context_t hctx, const char *name,
                            bufferlist *inbl);
extern int cls_cxx_stat(cls_method_context_t hctx, uint64_t *size, time_t *mtime);

extern int cls_register_cxx_method(cls_handle_t hclass, const char *method, int flags,
				   cls_method_cxx_call_t class_call, cls_method_handle_t *handle);

#endif

#endif
