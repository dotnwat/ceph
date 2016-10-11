// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_OBJCLASS_OBJCLASS_PUBLIC_H
#define CEPH_OBJCLASS_OBJCLASS_PUBLIC_H

#ifdef __cplusplus

#include <map>
#include <set>
#include "buffer.h"
using namespace std;

struct obj_list_watch_response_t;

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
#define CLS_METHOD_PUBLIC   0x4 /// unused
#define CLS_METHOD_PROMOTE  0x8 /// method cannot be proxied to base tier

#define CLS_LOG(level, fmt, ...)                                        \
  cls_log(level, "<cls> %s:%d: " fmt, __FILE__, __LINE__, ##__VA_ARGS__)
#define CLS_ERR(fmt, ...) CLS_LOG(0, fmt, ##__VA_ARGS__)

/**
 * Initialize a class.
 */
void __cls_init();

/**
 * @typdef cls_handle_t
 *
 * A handle for interacting with the object class.
 */
typedef void *cls_handle_t;

/**
 * @typedef cls_method_handle_t
 *
 * A handle for interacting with the method of the object class.
 */
typedef void *cls_method_handle_t;

/**
 * @typedef  cls_filter_handle
 *
 * A handle for interacting with the filter.
 */
typedef void *cls_filter_handle_t;

/**
 * @typedef cls_method_context_t
 *
 * A context for the method of the object class.
 */
typedef void* cls_method_context_t;

/**
 * @typedef cls_method_call_t
 *
 * A handle for the method call. 
 */
typedef int (*cls_method_call_t)(cls_method_context_t ctx,
                                 char *indata, int datalen,
                                char **outdata, int *outdatalen);

/**
 * @typedef cls_deps_t
 *
 * A handle for class dependency.
 */
typedef struct {
        const char *name;
        const char *ver;
} cls_deps_t;

/* class utils */

/**
 * Log object class information.
 *
 * @param level
 * @param format
 */
extern int cls_log(int level, const char *format, ...)
  __attribute__((__format__(printf, 2, 3)));

/* class registration api */

/**
 * Register the class.
 *
 * @param name
 * @param handle
 */
extern int cls_register(const char *name, cls_handle_t *handle);

/**
 * Unregister the class.
 *
 * @param handle
 *
 */
extern int cls_unregister(cls_handle_t);

/**
 * Register class method.
 *
 * @param hclass
 * @param method
 * @param flags
 * @param class_call
 * @param handle
 */
extern int cls_register_method(cls_handle_t hclass, const char *method, int flags,
                        cls_method_call_t class_call, cls_method_handle_t *handle);

/**
 * Unregister class method.
 *
 * @param handle
 */
extern int cls_unregister_method(cls_method_handle_t handle);

/**
 * Unregister class filter.
 *
 * @param handle
 */
extern void cls_unregister_filter(cls_filter_handle_t handle);

/* triggers */
#define OBJ_READ    0x1
#define OBJ_WRITE   0x2

/**
 * @typedef cls_trigger_t
 *
 */
typedef int cls_trigger_t;

/**
 * cls_link
 *
 * @param handle
 * @param trigger
 */
extern int cls_link(cls_method_handle_t handle, int priority, cls_trigger_t trigger);

/**
 * cls_unlink
 *
 * @param handle
 */
extern int cls_unlink(cls_method_handle_t handle);

/* should be defined by the class implementation
   defined here inorder to get it compiled without C++ mangling */
extern void class_init(void);
extern void class_fini(void);

#ifdef __cplusplus
}

/**
 * @typedef cls_method_cxx_call_t
 *
 */
typedef int (*cls_method_cxx_call_t)(cls_method_context_t ctx,
    class buffer::list *inbl, class buffer::list *outbl);

/**
 * @typedef void* cls_hobject_t
 *
 * May be useful to hide the implementation of hobject for any 
 * cls library that needs to be used as a self contained package
 * outside the Ceph src tree.
 * 
 */
typedef void* cls_hobject_t;

/**
 * Register a method.
 *
 * @param hclass
 * @param method
 * @param flags
 * @param class_call
 * @param handle
 */
extern int cls_register_cxx_method(cls_handle_t hclass, const char *method, int flags,
                                   cls_method_cxx_call_t class_call, cls_method_handle_t *handle);

/**
 * Register a filter.
 *
 * @param hclass
 * @param filtername
 * @param fn
 * @param handle
 */

/*
//cls_register_cxx_filter present in objclass.h
extern int cls_register_cxx_filter(cls_handle_t hclass,
                                   const std::string &filter_name,
                                   cls_cxx_filter_factory_t fn,
                                   cls_filter_handle_t *handle=NULL);
*/

/**
 * Create an object.
 *
 * @param hctx
 * @param exclusive
 */
extern int cls_cxx_create(cls_method_context_t hctx, bool exclusive);

/**
 * Remove an object.
 *
 * @param hctx
 */
extern int cls_cxx_remove(cls_method_context_t hctx);

/**
 * Check on the status of an object.
 * 
 * @param hctx
 * @param size
 * @param mtime
 */
extern int cls_cxx_stat(cls_method_context_t hctx, uint64_t *size, time_t *mtime);

/**
 * Check on the status of an object.
 *
 * This function is deprecated and any cls library that needs to be used as a self
 * contained package must not use this. Using the function will require includes from
 * within the Ceph src tree.
 *
 * @param hctx
 * @param size
 * @param time
 */
// changed ceph::real_time *mtime to void *time
extern int cls_cxx_stat2(cls_method_context_t hctx, uint64_t *size, void *time);
/**
 * Read contents of an object.
 *
 * @param hctx
 * @param ofs
 * @param len
 * @param bl
 */
extern int cls_cxx_read(cls_method_context_t hctx, int ofs, int len, bufferlist *bl);

/**
 * Read with additional flags.
 *
 * @param hctx
 * @param ofs
 * @param len
 * @param bl
 * @param op_flags
 */
extern int cls_cxx_read2(cls_method_context_t hctx, int ofs, int len,
                         bufferlist *bl, uint32_t op_flags);
/**
 * Write to the object.
 *
 * @param hctx
 * @param ofs
 * @param len
 * @param bl
 */
extern int cls_cxx_write(cls_method_context_t hctx, int ofs, int len, bufferlist *bl);

/**
 * Write with additional flags.
 *
 * @param hctx
 * @param ofs
 * @param len
 * @param bl
 * @param op_flags
 */
extern int cls_cxx_write2(cls_method_context_t hctx, int ofs, int len,
                         bufferlist *bl, uint32_t op_flags);

/**
 * write_full == truncate(0) + write
 *
 * @param hctx
 * @param bl
 */
extern int cls_cxx_write_full(cls_method_context_t hctx, bufferlist *bl);

/**
 * Get xattr of the object.
 *
 * @param hctx
 * @param name
 * @param outbl
 */
extern int cls_cxx_getxattr(cls_method_context_t hctx, const char *name,
                            bufferlist *outbl);

/**
 * Get xattrs which is in the form of a map.
 *
 * @param hctx
 * @param attrset
 */

extern int cls_cxx_getxattrs(cls_method_context_t hctx, map<string, bufferlist> *attrset);

/**
 * Set xattr of the object.
 *
 * @param hctx
 * @param name
 * @param inbl
 */
extern int cls_cxx_setxattr(cls_method_context_t hctx, const char *name,
                            bufferlist *inbl);
/**
 * Replace contents.
 *
 * @param hctx
 * @param ofs
 * @param len
 * @param bl
 */
extern int cls_cxx_replace(cls_method_context_t hctx, int ofs, int len, bufferlist *bl);

/**
 * Clear map.
 *
 * @param hctx
 */
extern int cls_cxx_map_clear(cls_method_context_t hctx);

/**
 * Get all values from the map.
 *
 * @param hctx
 * @param vals
 */
extern int cls_cxx_map_get_all_vals(cls_method_context_t hctx,
                                    std::map<string, bufferlist> *vals);
/**
 * Get keys from the map within a range.
 *
 * @param hctx
 * @param start_after
 * @param max_to_get
 * @param keys 
 */
extern int cls_cxx_map_get_keys(cls_method_context_t hctx,
                                const string &start_after,
                                uint64_t max_to_get,
                                std::set<string> *keys);

/**
 * Get values from the map within a range.
 *
 * @param hctx
 * @param start_after
 * @param filter_prefix
 * @param max_to_get
 * @param vals
 */
extern int cls_cxx_map_get_vals(cls_method_context_t hctx,
                                const std::string &start_after,
                               const std::string &filter_prefix,
                               uint64_t max_to_get,
                               std::map<string, bufferlist> *vals);

/**
 * Read header from the map.
 *
 * @param hctx
 * @param outbl
 */
extern int cls_cxx_map_read_header(cls_method_context_t hctx, bufferlist *outbl);

/**
 * Get value corresponding to a key from the map.
 *
 * @param hctx
 * @param key
 * @param outbl
 */
extern int cls_cxx_map_get_val(cls_method_context_t hctx,
                               const std::string &key, bufferlist *outbl);

/**
 * Set value corresponding to a key in the map.
 *
 * @param hctx
 * @param key
 * @param inbl
 */
extern int cls_cxx_map_set_val(cls_method_context_t hctx,
                               const std::string &key, bufferlist *inbl);

/**
 * Set values in the map.
 *
 * @param hctx
 * @param map
 */
extern int cls_cxx_map_set_vals(cls_method_context_t hctx,
                                const std::map<string, bufferlist> *map);

/**
 * Write header to the map.
 *
 * @param hctx
 * @param inbl
 */
extern int cls_cxx_map_write_header(cls_method_context_t hctx, bufferlist *inbl);

/**
 * Remove key from the map.
 *
 * @param hctx
 * @param key
 */
extern int cls_cxx_map_remove_key(cls_method_context_t hctx, const std::string &key);

/**
 * Update map.
 *
 * @param hctx
 * @param inbl
 */
extern int cls_cxx_map_update(cls_method_context_t hctx, bufferlist *inbl);

/**
 * List watchers. 
 *
 * @param hctx
 * @param watchers
 */
extern int cls_cxx_list_watchers(cls_method_context_t hctx,
                                 obj_list_watch_response_t *watchers);

/* utility functions */

/**
 * Generate random bytes.
 *
 * @param buf
 * @param size
 */
extern int cls_gen_random_bytes(char *buf, int size);

/**
 * Generate random base64.
 *
 * @param dest
 * @param size
 */
extern int cls_gen_rand_base64(char *dest, int size); /* size should be the required string size + 1 */

/* environment */

/**
 * Get last user object version.
 *
 * @param hctx
 */
extern uint64_t cls_current_version(cls_method_context_t hctx);

/**
 * Get current sub operation number.
 *
 * @param hctx
 */
extern int cls_current_subop_num(cls_method_context_t hctx);

/**
 * Get OSD features.
 *
 * @param hctx
 */
extern uint64_t cls_get_features(cls_method_context_t hctx);

/* helpers */

/**
 * Get sub operation version.
 *
 * @param hctx
 * @param s
 */
extern void cls_cxx_subop_version(cls_method_context_t hctx, std::string *s);

/* These are also defined in rados.h and librados.h. Keep them in sync! */
#define CEPH_OSD_TMAP_HDR 'h'
#define CEPH_OSD_TMAP_SET 's'
#define CEPH_OSD_TMAP_CREATE 'c'
#define CEPH_OSD_TMAP_RM 'r'
#endif

#endif
