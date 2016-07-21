// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

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

/**
 * @typdef cls_handle_t
 *
 * A handle for interacting with the object class.
 */
typedef void* cls_handle_t;

/**
 * @typedef cls_method_handle_t
 *
 * A handle for interacting with the method of the object class.
 */
typedef void* cls_method_handle_t;

/**
 * @typedef cls_method_context_t
 *
 * A context for the method of the object class.
 */
typedef void* cls_method_context_t;

typedef void* entity_origin_ptr_t;

 /* class utils */
extern int cls_log(int level, const char *format, ...)
  __attribute__((__format__(printf, 2, 3)));
/** This will fill in the passed origin pointer with the origin of the
 * request which activated your class call. */

extern int cls_get_request_origin2(cls_method_context_t hctx,
     entity_origin_ptr_t *origin);
extern int cls_serialize(entity_origin_ptr_t origin,
    std::string *origin_string);
//extern int cls_get_request_origin(cls_method_context_t hctx,
//                                  entity_inst_t *origin);

/* class registration api */
extern int cls_register(const char *name, cls_handle_t *handle);
#ifdef __cplusplus
}

typedef int (*cls_method_cxx_call_t)(cls_method_context_t ctx,
    class buffer::list *inbl, class buffer::list *outbl);

#if 0

typedef void* cls_hobject_t;

class PGLSFilter {
protected:
  string xattr;
public:
  PGLSFilter();
  virtual ~PGLSFilter();
  virtual bool filter(cls_hobject_t obj, bufferlist& xattr_data,
                      bufferlist& outdata) = 0;
  /**
   * Arguments passed from the RADOS client.  Implementations must
   * handle any encoding errors, and return an appropriate error code,
   * or 0 on valid input.
   */
  virtual int init(bufferlist::iterator &params) = 0;

  /**
   * xattr key, or empty string.  If non-empty, this xattr will be fetched
   * and the value passed into ::filter
   */
   virtual string& get_xattr() { return xattr; }

  /**
   * If true, objects without the named xattr (if xattr name is not empty)
   * will be rejected without calling ::filter
   */
  virtual bool reject_empty_xattr() { return true; }
};

// Classes expose a filter constructor that returns a subclass of PGLSFilter
typedef PGLSFilter* (*cls_cxx_filter_factory_t)();

#endif

/**
 * Register the object class method.
 *
 * @param hclass
 * @param method
 * @param flags
 * @param class_call
 * @param handle
 */
extern int cls_register_cxx_method(cls_handle_t hclass, const char *method, int flags,
                                   cls_method_cxx_call_t class_call, cls_method_handle_t *handle);
//extern int cls_register_cxx_filter(cls_handle_t hclass,
//                                   const std::string &filter_name,
//                                   cls_cxx_filter_factory_t fn,
//                                   cls_filter_handle_t *handle=NULL);
/**
 * Check on the status of the object class method.
 *
 * @param hctx
 * @param size
 * @param mtime
 */
extern int cls_cxx_stat(cls_method_context_t hctx, uint64_t *size, time_t *mtime);
extern int cls_cxx_read(cls_method_context_t hctx, int ofs, int len, bufferlist *bl);
extern int cls_cxx_write_full(cls_method_context_t hctx, bufferlist *bl);
extern int cls_cxx_setxattr(cls_method_context_t hctx, const char *name,
                            bufferlist *inbl);
#endif

#endif
