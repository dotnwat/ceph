// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_OBJCLASS_H
#define CEPH_OBJCLASS_H

#include "../include/types.h"
#include "msg/msg_types.h"
#include "common/hobject.h"
#include "common/ceph_time.h"

#include "objclass-public.h"

class PGLSFilter {
protected:
  string xattr;
public:
  PGLSFilter();
  virtual ~PGLSFilter();
  virtual bool filter(const hobject_t &obj, bufferlist& xattr_data,
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

extern int cls_register_cxx_filter(cls_handle_t hclass,
                                   const std::string &filter_name,
				   cls_cxx_filter_factory_t fn,
                                   cls_filter_handle_t *handle=NULL);
extern int cls_cxx_snap_revert(cls_method_context_t hctx, snapid_t snapid);

#endif
