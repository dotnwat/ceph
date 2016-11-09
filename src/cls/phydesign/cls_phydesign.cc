#include <errno.h>
#include <string>
#include <sstream>
#include "include/types.h"
#include "objclass/objclass.h"
#include "cls_phydesign.h"

CLS_VER(1,0)
CLS_NAME(phydesign)

cls_handle_t h_class;
cls_method_handle_t h_zlog;

/*
 * metadata/index entry for when the log entry data is stored in the
 * bytestream. it records state (e.g. filled, trimmed, etc...) and the
 * location and size of the entry.
 */
struct entry {
  int flags;
  uint64_t offset;
  uint64_t length;

  entry() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(flags, bl);
    ::encode(offset, bl);
    ::encode(length, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(flags, bl);
    ::decode(offset, bl);
    ::decode(length, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(entry)

static inline std::string u64tostr(uint64_t value)
{
  std::stringstream ss;
  ss << "pos." << std::setw(20) << std::setfill('0') << value;
  return ss.str();
}

static std::string ops_to_string(const std::vector<int>& ops)
{
  std::stringstream ss;

  for (std::vector<int>::const_iterator it = ops.begin();
       it != ops.end(); it++) {
    switch (*it) {

      case INIT_STATE:
        ss << "init_state";
        break;


      case READ_EPOCH_OMAP:
        ss << "read_epoch_omap";
        break;
      case READ_EPOCH_OMAP_HDR:
        ss << "read_epoch_omap_hdr";
        break;
      case READ_EPOCH_XATTR:
        ss << "read_epoch_xattr";
        break;
      case READ_EPOCH_NOOP:
        ss << "read_epoch_noop";
        break;
      case READ_EPOCH_HEADER:
        ss << "read_epoch_header";
        break;

      case READ_OMAP_INDEX_ENTRY:
        ss << "read_omap_index_entry";
        break;
      case WRITE_OMAP_INDEX_ENTRY:
        ss << "write_omap_index_entry";
        break;

      case APPEND_DATA:
        ss << "append_data";
        break;

      default:
        ss << "unknown";
        break;
    }
    ss << " ";
  }

  return ss.str();
}

static int zlog(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  std::vector<int> ops;
  uint64_t position;
  bufferlist entry_blob;

  try {
    bufferlist::iterator it = in->begin();
    ::decode(ops, it);
    ::decode(position, it);
    ::decode(entry_blob, it);
  } catch (const buffer::error &err) {
    CLS_ERR("ERROR: decoding zlog ops");
    return -EINVAL;
  }

#if 0
  std::string op_str = ops_to_string(ops);
  CLS_LOG(0, "ops: %s", op_str.c_str());
  CLS_LOG(0, "pos: %d", (int)position);
  CLS_LOG(0, "len: %d", (int)entry_blob.length());
#endif

  for (std::vector<int>::const_iterator it = ops.begin();
       it != ops.end(); it++) {
    const int op = *it;
    switch (op) {
      case INIT_STATE:
        {
          int ret = cls_cxx_create(hctx, false);
          if (ret < 0) {
            CLS_ERR("ERROR: INIT_STATE/create %d", ret);
            return ret;
          }

          uint64_t epoch = 0;
          bufferlist bl;
          ::encode(epoch, bl);

          ret = cls_cxx_map_set_val(hctx, "epoch", &bl);
          if (ret < 0) {
            CLS_ERR("ERROR: INIT_STATE/setval %d", ret);
            return ret;
          }

          bl.clear();
          ::encode(epoch, bl);

          ret = cls_cxx_map_write_header(hctx, &bl);
          if (ret < 0) {
            CLS_ERR("ERROR: INIT_STATE/write_header %d", ret);
            return ret;
          }

          bl.clear();
          ::encode(epoch, bl);

          ret = cls_cxx_setxattr(hctx, "epoch", &bl);
          if (ret < 0) {
            CLS_ERR("ERROR: INIT_STATE/setxattr %d", ret);
            return ret;
          }

          bl.clear();
          char header[4096];
          bl.append(header, sizeof(header));

          ret = cls_cxx_write(hctx, 0, bl.length(), &bl);
          if (ret < 0) {
            CLS_ERR("ERROR: INIT_STATE/write %d", ret);
            return ret;
          }
        }
        break;

      case READ_EPOCH_OMAP:
        {
          bufferlist bl;
          int ret = cls_cxx_map_get_val(hctx, "epoch", &bl);
          if (ret < 0) {
            CLS_ERR("ERROR: READ_EPOCH_OMAP/getval %d", ret);
            return ret;
          }
        }
        break;

      case READ_EPOCH_OMAP_HDR:
        {
          bufferlist bl;
          int ret = cls_cxx_map_read_header(hctx, &bl);
          if (ret < 0) {
            CLS_ERR("ERROR: READ_EPOCH_OMAP_HDR/read_header %d", ret);
            return ret;
          }
        }
        break;

      case READ_EPOCH_XATTR:
        {
          bufferlist bl;
          int ret = cls_cxx_getxattr(hctx, "epoch", &bl);
          if (ret < 0) {
            CLS_ERR("ERROR: READ_EPOCH_XATTR/getxattr %d", ret);
            return ret;
          }
        }
        break;

      case READ_EPOCH_NOOP:
        break;

      case READ_EPOCH_HEADER:
        {
          uint64_t epoch;
          bufferlist bl;
          int ret = cls_cxx_read(hctx, 0, sizeof(epoch), &bl);
          if (ret < 0) {
            CLS_ERR("ERROR: READ_EPOCH_XATTR/getxattr %d", ret);
            return ret;
          }
        }
        break;

      case READ_OMAP_INDEX_ENTRY:
        {
          bufferlist bl;
          const std::string key = u64tostr(position);
          int ret = cls_cxx_map_get_val(hctx, key, &bl);
          if (ret < 0 && ret != -ENOENT) {
            CLS_ERR("ERROR: READ_OMAP_INDEX_ENTRY/getval %d", ret);
            return ret;
          }
        }
        break;

      case WRITE_OMAP_INDEX_ENTRY:
        {
          struct entry e;
          bufferlist bl;
          ::encode(e, bl);
          const std::string key = u64tostr(position);
          int ret = cls_cxx_map_set_val(hctx, key, &bl);
          if (ret < 0) {
            CLS_ERR("ERROR: WRITE_OMAP_INDEX_ENTRY/setval %d", ret);
            return ret;
          }
        }
        break;

      case APPEND_DATA:
        {
          uint64_t size;
          int ret = cls_cxx_stat(hctx, &size, NULL);
          if (ret < 0) {
            CLS_ERR("ERROR: APPEND_DATA/stat %d", ret);
            return ret;
          }

          ret = cls_cxx_write(hctx, size, entry_blob.length(), &entry_blob);
          if (ret < 0) {
            CLS_ERR("ERROR: APPEND_DATA/write %d", ret);
            return ret;
          }
        }
        break;

      default:
        CLS_ERR("ERROR: invalid op %d", op);
        return -EINVAL;
    }
  }

  return 0;
}

void __cls_init()
{
  CLS_LOG(20, "Loaded physical design class!");

  cls_register("phydesign", &h_class);

  cls_register_cxx_method(h_class, "zlog",
      CLS_METHOD_RD | CLS_METHOD_WR, zlog, &h_zlog);
}
