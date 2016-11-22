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
cls_method_handle_t h_zlog_batch_write_simple;
cls_method_handle_t h_zlog_batch_write_batch;
cls_method_handle_t h_zlog_batch_write_batch_oident;

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

#if 0
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
#endif

static int __zlog_batch_write_batch(cls_method_context_t hctx, bufferlist *in, bufferlist *out,
    bool identify_outlier)
{
  uint64_t epoch;
  std::vector<entry_info> entries;

  try {
    bufferlist::iterator it = in->begin();
    ::decode(epoch, it);
    ::decode(entries, it);
  } catch (const buffer::error &err) {
    CLS_ERR("ERROR: decoding ops");
    return -EINVAL;
  }

  size_t num_entries = entries.size();
  if (!num_entries) {
    CLS_ERR("ERROR: entries list is empty");
    return -EINVAL;
  }

  // simulate check epoch stored in omap header
  bufferlist hdr_bl;
  int ret = cls_cxx_map_read_header(hctx, &hdr_bl);
  if (ret < 0) {
    CLS_ERR("ERROR: map_read__header %d", ret);
    return ret;
  }

  // figure out the bounds of the omap query
  uint64_t min_pos, max_pos;
  min_pos = max_pos = entries[0].position;
  for (unsigned i = 1; i < num_entries; i++) {
    min_pos = std::min(entries[i].position, min_pos);
    max_pos = std::max(entries[i].position, max_pos);
  }
  // add in the outlier to the query range. see the simple batch method for an
  // explanation of the epoch/outlier usage.
  if (!identify_outlier && epoch > 0)
    min_pos = std::min(epoch, min_pos);
  min_pos = std::max(min_pos, (uint64_t)1); // just safety for 0 case below
  if (min_pos > max_pos) {
    CLS_ERR("min_pos %llu > max_pos %llu",
        (unsigned long long)min_pos,
        (unsigned long long)max_pos);
    return -EINVAL;
  }

#if 0
  CLS_ERR("ABC1: oident %d num_entries %d min_pos %d max_pos %d",
      (int)identify_outlier, (int)num_entries, (int)min_pos, (int)max_pos);
#endif

  if (identify_outlier && epoch > 0) {
    // read the metadata associated with the position from omap
    bufferlist key_bl;
    const std::string key = u64tostr(epoch);
    int ret = cls_cxx_map_get_val(hctx, key, &key_bl);
    (void)ret; // ignore return value
#if 0
    CLS_ERR("ABC3: fetching key for OUTLIER pos %s", key.c_str());
#endif
  }

  // get all the entries for the span
  std::map<std::string, ceph::bufferlist> stored_entries;
  uint64_t count = max_pos - min_pos + 1;
  const std::string& start_after = u64tostr(min_pos - 1);
  ret = cls_cxx_map_get_vals(hctx, start_after, "", count,
      &stored_entries);
  if (ret < 0) {
    CLS_ERR("ERROR: map get vals %d", ret);
    return ret;
  }

  // get object size for append offset
  uint64_t size;
  ret = cls_cxx_stat(hctx, &size, NULL);
  if (ret < 0) {
    CLS_ERR("ERROR: stat %d", ret);
    return ret;
  }

  // bytestream append blob
  ceph::bufferlist append_bl;
  // omap index updates
  std::map<std::string, ceph::bufferlist> entries_update;

  uint64_t offset = size;
  for (auto& entry : entries) {
    const std::string key = u64tostr(entry.position);

    auto it = stored_entries.find(key);
    if (it != stored_entries.end()) {
      CLS_ERR("ERROR: should have found an entry %llu",
          (unsigned long long)entry.position);
      return -EINVAL;
    }

    // create and add entry to update set
    ceph::bufferlist entry_bl;
    struct entry e;
    e.offset = offset;
    e.length = entry.data.length();
    ::encode(e, entry_bl);
    entries_update[key] = entry_bl;

    // update the append blob
    append_bl.claim_append(entry.data, 0);

    offset += e.length;
  }

#if 0
  std::stringstream ss;
  for (auto it = entries_update.begin();
       it != entries_update.end(); it++) {
    ss << it->first << " ";
  }

  const std::string entry_summary = ss.str();

  CLS_ERR("XBC write %llu entries, off %llu, size %llu keys %s",
      (unsigned long long)entries_update.size(),
      (unsigned long long)size,
      (unsigned long long)append_bl.length(),
      entry_summary.c_str());
#endif

  // append entry payloads to object
  ret = cls_cxx_write(hctx, size, append_bl.length(), &append_bl);
  if (ret < 0) {
    CLS_ERR("ERROR: write %d", ret);
    return ret;
  }

  // update metadata
  ret = cls_cxx_map_set_vals(hctx, &entries_update);
  if (ret < 0) {
    CLS_ERR("ERROR: map set vals %d", ret);
    return ret;
  }

  return 0;
}

static int zlog_batch_write_batch(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  return __zlog_batch_write_batch(hctx, in, out, false);
}

static int zlog_batch_write_batch_oident(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  return __zlog_batch_write_batch(hctx, in, out, true);
}

/*
 * simple
 * batch omap/write
 * batch omap with outlier
 */
static int zlog_batch_write_simple(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  uint64_t epoch;
  std::vector<entry_info> entries;

  try {
    bufferlist::iterator it = in->begin();
    ::decode(epoch, it);
    ::decode(entries, it);
  } catch (const buffer::error &err) {
    CLS_ERR("ERROR: decoding ops");
    return -EINVAL;
  }

  if (entries.empty()) {
    CLS_ERR("ERROR: entries list is empty");
    return -EINVAL;
  }

  // simulate check epoch stored in omap header
  bufferlist bl;
  int ret = cls_cxx_map_read_header(hctx, &bl);
  if (ret < 0) {
    CLS_ERR("ERROR: map_read__header %d", ret);
    return ret;
  }

  /*
   * We've added a temporary hack by which the epoch parameter is used to
   * transport the outlier parameter... the epoch isnt actually used in these
   * prototypes so whatever. The outlier is a position we are going to query
   * from omap, but treat as a failed append.
   */
  if (epoch > 0) {
    // read the metadata associated with the position from omap
    bufferlist key_bl;
    const std::string key = u64tostr(epoch);
    int ret = cls_cxx_map_get_val(hctx, key, &key_bl);
    (void)ret; // ignore return value
#if 0
    CLS_ERR("ABC2: fetching key for OUTLIER pos %s", key.c_str());
#endif
  }

  for (auto& entry : entries) {
    // read the metadata associated with the position from omap
    bufferlist key_bl;
    const std::string key = u64tostr(entry.position);
    int ret = cls_cxx_map_get_val(hctx, key, &key_bl);
    if (ret < 0 && ret != -ENOENT) {
      CLS_ERR("ERROR: getval %d", ret);
      return ret;
    }

#if 0
    CLS_ERR("ABC2: ENTRY POS NEXT NORMAL %s", key.c_str());
#endif

    // get object size for append
    uint64_t size;
    ret = cls_cxx_stat(hctx, &size, NULL);
    if (ret < 0) {
      CLS_ERR("ERROR: stat %d", ret);
      return ret;
    }

    // append data payload to object
    ret = cls_cxx_write(hctx, size, entry.data.length(), &entry.data);
    if (ret < 0) {
      CLS_ERR("ERROR: write %d", ret);
      return ret;
    }

    // prepare metadata update
    key_bl.clear();
    struct entry e;
    e.offset = size;
    e.length = entry.data.length();
    ::encode(e, bl);

    ret = cls_cxx_map_set_val(hctx, key, &key_bl);
    if (ret < 0) {
      CLS_ERR("ERROR: setval %d", ret);
      return ret;
    }
  }

  return 0;
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

  cls_register_cxx_method(h_class, "zlog_batch_write_simple",
      CLS_METHOD_RD | CLS_METHOD_WR, zlog_batch_write_simple,
      &h_zlog_batch_write_simple);

  cls_register_cxx_method(h_class, "zlog_batch_write_batch",
      CLS_METHOD_RD | CLS_METHOD_WR, zlog_batch_write_batch,
      &h_zlog_batch_write_batch);

  cls_register_cxx_method(h_class, "zlog_batch_write_batch_oident",
      CLS_METHOD_RD | CLS_METHOD_WR, zlog_batch_write_batch_oident,
      &h_zlog_batch_write_batch_oident);
}
