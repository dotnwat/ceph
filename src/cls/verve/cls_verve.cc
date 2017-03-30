#include "include/int_types.h"
#include <boost/lexical_cast.hpp>
#include <vector>
#include "objclass/objclass.h"
#include "verve.pb.h"

CLS_VER(1,0)
CLS_NAME(verve)

static inline std::string u64tostr(const std::string& prefix, uint64_t value)
{
  std::stringstream ss;
  ss << prefix << std::setw(20) << std::setfill('0') << value;
  return ss.str();
}

static void pb_encode(bufferlist& bl, google::protobuf::Message& msg)
{
  assert(msg.IsInitialized());
  char buf[msg.ByteSize()];
  assert(msg.SerializeToArray(buf, sizeof(buf)));
  bl.append(buf, sizeof(buf));
}

static bool pb_decode(bufferlist& bl, google::protobuf::Message* msg)
{
  if (bl.length() == 0) {
    return false;
  }
  if (!msg->ParseFromString(bl.to_str())) {
    std::cerr << "decode: unable to decode message" << std::endl;
    return false;
  }
  if (!msg->IsInitialized()) {
    std::cerr << "decode: message is uninitialized" << std::endl;
    return false;
  }
  return true;
}

static int read_locks(cls_method_context_t hctx, verve::Locks& locks)
{
  bufferlist bl;
  int ret = cls_cxx_map_read_header(hctx, &bl);
  if (ret < 0) {
    switch (ret) {
      case -ENODATA:
      case -ENOENT:
        break;
      default:
        return ret;
    }
  }

  if (bl.length() > 0) {
    if (!pb_decode(bl, &locks)) {
      CLS_ERR("ERROR: read_locks: failed to decode locks");
      return -EIO;
    }
  }

  return 0;
}

/*
 * a transaction with internally conflicting locks will fail as if the
 * conflicts locks were already taken.
 *
 * if a conflict exists we immediately return. we don't need to actually
 * remove the locks from the data structure to release them because returning
 * an error will result in the state change being tossed out.
 *
 * this could be optimized for cases where there are a lot of locks.
 *
 * zero length lock?
 *
 * remove code redundancy
 */
static int try_lock_txn(cls_method_context_t hctx, const verve::ExecuteAndPrepare& txn,
    verve::Locks& locks)
{
  // read current object locks
  int ret = read_locks(hctx, locks);
  if (ret < 0) {
    CLS_ERR("ERROR: try_lock_txn: failed to read locks: %d", ret);
    return ret;
  }

  // try read locks
  for (int i = 0; i < txn.reads_size(); i++) {
    const verve::ByteExtent& read = txn.reads(i);
    for (int j = 0; j < locks.write_locks_size(); j++) {
      const verve::ByteExtent& lock = locks.write_locks(j);
      if (read.offset() < (lock.offset() + lock.length()) &&
          lock.offset() < (read.offset() + read.length())) {
        return -EBUSY;
      }
    }
  }
  for (int i = 0; i < txn.reads_size(); i++) {
    const verve::ByteExtent& read = txn.reads(i);
    verve::ByteExtent *lock = locks.add_read_locks();
    lock->set_offset(read.offset());
    lock->set_length(read.length());
  }

  // try compare locks
  for (int i = 0; i < txn.compares_size(); i++) {
    const verve::ByteExtent& read = txn.compares(i).extent();
    for (int j = 0; j < locks.write_locks_size(); j++) {
      const verve::ByteExtent& lock = locks.write_locks(j);
      if (read.offset() < (lock.offset() + lock.length()) &&
          lock.offset() < (read.offset() + read.length()))
        return -EBUSY;
    }
  }
  for (int i = 0; i < txn.compares_size(); i++) {
    const verve::ByteExtent& read = txn.compares(i).extent();
    verve::ByteExtent *lock = locks.add_read_locks();
    lock->set_offset(read.offset());
    lock->set_length(read.length());
  }

  // try write locks
  for (int i = 0; i < txn.writes_size(); i++) {
    const verve::ByteExtent& write = txn.writes(i).extent();
    for (int j = 0; j < locks.write_locks_size(); j++) {
      const verve::ByteExtent& lock = locks.write_locks(j);
      if (write.offset() < (lock.offset() + lock.length()) &&
          lock.offset() < (write.offset() + write.length()))
        return -EBUSY;
    }
    for (int j = 0; j < locks.read_locks_size(); j++) {
      const verve::ByteExtent& lock = locks.read_locks(j);
      if (write.offset() < (lock.offset() + lock.length()) &&
          lock.offset() < (write.offset() + write.length()))
        return -EBUSY;
    }
  }
  for (int i = 0; i < txn.writes_size(); i++) {
    const verve::ByteExtent& write = txn.writes(i).extent();
    verve::ByteExtent *lock = locks.add_write_locks();
    lock->set_offset(write.offset());
    lock->set_length(write.length());
  }

  return 0;
}

static int exec_and_prepare(cls_method_context_t hctx, bufferlist *in,
    bufferlist *out)
{
  verve::ExecuteAndPrepare txn;
  if (!pb_decode(*in, &txn)) {
    CLS_ERR("ERROR: exec_and_prepare: failed to decode input");
    return -EINVAL;
  }

  // try to grab read and write locks
  verve::Locks locks;
  int ret = try_lock_txn(hctx, txn, locks);
  if (ret < 0)
    return ret;

  // evaluate comparisons
  for (int i = 0; i < txn.compares_size(); i++) {
    const verve::ByteExtentValue& cmp = txn.compares(i);
    const verve::ByteExtent& read = cmp.extent();

    bufferlist bl;
    int ret = cls_cxx_read(hctx, read.offset(), read.length(), &bl);
    if (ret < 0) {
      CLS_ERR("ERROR: exec_and_prepare: failed to read %d", ret);
      return ret;
    }

    const std::string& value = cmp.value();
    if (value.size() != bl.length())
      return -EFAULT;

    if (memcmp(value.c_str(), bl.c_str(), value.size()))
      return -EFAULT;
  }

  /*
   * since we cannot return data to the client in this operation because it
   * also performs writes, we are going to stash a copy of the read data under
   * a key for the client to retrieve later.
   *
   *   - how compelling is it for ceph to keep this restriction?
   *
   *   - since we have read locks, can we let the client perform reads
   *   directly? it seems as though we'd need to verify the integrity of the
   *   reads with respect to other actions that could occur in-between taking
   *   the lock and performing the read (e.g. recovery protocol forcing
   *   abort?)
   */
  verve::Reads reads;
  for (int i = 0; i < txn.reads_size(); i++) {
    const verve::ByteExtent& read = txn.reads(i);

    bufferlist bl;
    int ret = cls_cxx_read(hctx, read.offset(), read.length(), &bl);
    if (ret < 0) {
      CLS_ERR("ERROR: exec_and_prepare: failed to read %d", ret);
      return ret;
    }

    if (bl.length() != read.length()) {
      CLS_ERR("ERROR: exec_and_prepare: failed to read requested extent");
      return -EINVAL;
    }

    verve::ByteExtentValue *read_value = reads.add_read();
    read_value->mutable_extent()->set_offset(read.offset());
    read_value->mutable_extent()->set_length(read.length());
    read_value->set_value(bl.c_str(), bl.length());
  }

  // save reads and txn
  std::map<string, bufferlist> map_updates;

  bufferlist txn_bl;
  pb_encode(txn_bl, txn);
  std::string txn_key = u64tostr("txn", txn.tid());
  map_updates[txn_key] = txn_bl;

  bufferlist reads_bl;
  pb_encode(reads_bl, reads);
  std::string read_key = u64tostr("read", txn.tid());
  map_updates[read_key] = reads_bl; 

  ret = cls_cxx_map_set_vals(hctx, &map_updates);
  if (ret < 0) {
    CLS_ERR("ERROR: exec_and_prepare: failed to update omap %d", ret);
    return ret;
  }

  // save updated locks
  bufferlist locks_bl;
  pb_encode(locks_bl, locks);
  ret = cls_cxx_map_write_header(hctx, &locks_bl);
  if (ret < 0) {
    CLS_ERR("ERROR: exec_and_prepare: failed to write header %d", ret);
    return ret;
  }

  return 0;
}

CLS_INIT(verve)
{
  CLS_LOG(0, "loading cls_verve");

  cls_handle_t h_class;
  cls_method_handle_t h_exec_and_prepare;

  cls_register("verve", &h_class);

  cls_register_cxx_method(h_class, "exec_and_prepare",
      CLS_METHOD_RD | CLS_METHOD_WR, exec_and_prepare,
      &h_exec_and_prepare);
}
