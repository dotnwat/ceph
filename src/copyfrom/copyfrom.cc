#include <iostream>
#include <vector>
#include <thread>
#include <mutex>
#include <sstream>
#include <fstream>
#include <boost/program_options.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include "include/rados/librados.hpp"

namespace po = boost::program_options;

static void do_gendata(librados::IoCtx& ioctx,
    size_t num_objs, size_t obj_size)
{
  // create random data to use for payloads
  const size_t rand_buf_size = 1ULL<<24;
  std::string rand_buf;
  rand_buf.reserve(rand_buf_size);
  std::ifstream ifs("/dev/urandom", std::ios::binary | std::ios::in);
  std::copy_n(std::istreambuf_iterator<char>(ifs),
      rand_buf_size, std::back_inserter(rand_buf));
  const char *rand_buf_raw = rand_buf.c_str();

  // grab random slices from the random bytes
  const size_t BLOCK_SIZE = 4096;
  std::default_random_engine generator;
  std::uniform_int_distribution<int> rand_dist;
  rand_dist = std::uniform_int_distribution<int>(0,
      rand_buf_size - BLOCK_SIZE - 1);

  for (size_t i = 0; i < num_objs; i++) {
    // generate random object data
    ceph::bufferlist bl;
    size_t left = obj_size;
    while (left) {
      size_t copy_size = std::min(left, BLOCK_SIZE);
      size_t buf_offset = rand_dist(generator);
      bl.append(rand_buf_raw + buf_offset, copy_size);
      left -= copy_size;
    }
    assert(bl.length() == obj_size);

    // generate source object name
    std::stringstream ss;
    ss << "copyfrom.src." << i;
    const std::string oid = ss.str();

    fprintf(stdout, "writing object: %s\r", oid.c_str());
    fflush(stdout);

    int ret = ioctx.remove(oid);
    assert(ret == 0 || ret == -ENOENT);

    ret = ioctx.write_full(oid, bl);
    assert(ret == 0);
  }

  fprintf(stdout, "\n");
  fflush(stdout);
}

static void get_source_objects(librados::IoCtx& ioctx,
    std::vector<std::string>& oids)
{
  int total_objs = 0;
  int total_src_objs = 0;
  const std::string prefix = "copyfrom.src.";

  std::vector<std::string> out;
  for (auto it = ioctx.nobjects_begin(); it != ioctx.nobjects_end(); it++) {
    const std::string oid = it->get_oid();
    auto mm = std::mismatch(prefix.begin(), prefix.end(), oid.begin());
    if (mm.first == prefix.end()) {
      out.push_back(oid);
      total_src_objs++;
    }
    total_objs++;
    std::cout << "searching for source objects... " <<
      total_src_objs << "/" << total_objs << "\r";
  }
  std::cout << std::endl;

  oids.swap(out);
}

static void naive_copy_worker(int i, librados::IoCtx *ioctx,
    std::vector<std::string> *oids, std::mutex *lock)
{
  boost::uuids::uuid uuid = boost::uuids::random_generator()();

  for (;;) {
    lock->lock();
    if (oids->empty()) {
      lock->unlock();
      break;
    }

    // src oid
    std::string src_oid = oids->back();
    oids->pop_back();
    lock->unlock();

    // dst oid
    std::stringstream dst_ss;
    dst_ss << "copyfrom.dst." << uuid << "." << src_oid;
    std::string dst_oid = dst_ss.str();

    ceph::bufferlist bl;
    int ret = ioctx->read(src_oid, bl, 0, 0);
    assert(ret > 0);
    assert(bl.length() > 0);

    ret = ioctx->stat(dst_oid, NULL, NULL);
    assert(ret == -ENOENT);

    ret = ioctx->write_full(dst_oid, bl);
    assert(ret == 0);
  }
}

static void do_naive_copy(librados::IoCtx& ioctx, int qdepth)
{
  // get a list of objects to copy
  std::vector<std::string> src_oids;
  get_source_objects(ioctx, src_oids);

  std::mutex lock;

  std::vector<std::thread> workers;
  for (int i = 0; i < qdepth; i++) {
    workers.push_back(std::thread(naive_copy_worker, i, &ioctx,
          &src_oids, &lock));
  }

  std::for_each(workers.begin(), workers.end(),
      [](std::thread& t) { t.join(); });
}

static void server_copy_worker(int i, librados::IoCtx *ioctx,
    std::vector<std::string> *oids, std::mutex *lock)
{
  boost::uuids::uuid uuid = boost::uuids::random_generator()();

  for (;;) {
    lock->lock();
    if (oids->empty()) {
      lock->unlock();
      break;
    }

    // src oid
    std::string src_oid = oids->back();
    oids->pop_back();
    lock->unlock();

    // dst oid
    std::stringstream dst_ss;
    dst_ss << "copyfrom.dst." << uuid << "." << src_oid;
    std::string dst_oid = dst_ss.str();

    int ret = ioctx->stat(dst_oid, NULL, NULL);
    assert(ret == -ENOENT);

    uint64_t ver = ioctx->get_last_version();

    librados::ObjectWriteOperation op;
    op.copy_from(src_oid, *ioctx, ver);

    ret = ioctx->operate(dst_oid, &op);
    assert(ret == 0);
  }
}

static void do_server_copy(librados::IoCtx& ioctx, int qdepth)
{
  // get a list of objects to copy
  std::vector<std::string> src_oids;
  get_source_objects(ioctx, src_oids);

  std::mutex lock;

  std::vector<std::thread> workers;
  for (int i = 0; i < qdepth; i++) {
    workers.push_back(std::thread(server_copy_worker, i, &ioctx,
          &src_oids, &lock));
  }

  std::for_each(workers.begin(), workers.end(),
      [](std::thread& t) { t.join(); });
}

int main(int argc, char **argv)
{
  size_t num_objs;
  size_t obj_size;
  std::string pool;
  bool gendata;
  int qdepth;
  bool copy_client;
  bool copy_server;

  po::options_description gen_opts("General options");
  gen_opts.add_options()
    ("help,h", "show help message")
    ("pool", po::value<std::string>(&pool)->required(), "rados pool")
    ("num-objs", po::value<size_t>(&num_objs)->default_value(0), "number of objects")
  ;

  po::options_description copy_opts("Copy workload options");
  copy_opts.add_options()
    ("copy-client", po::bool_switch(&copy_client)->default_value(false), "client copy mode")
    ("copy-server", po::bool_switch(&copy_server)->default_value(false), "server copy mode")
    ("qdepth", po::value<int>(&qdepth)->default_value(1), "queue depth")
  ;

  po::options_description datagen_opts("Source data generator options");
  datagen_opts.add_options()
    ("gendata", po::bool_switch(&gendata)->default_value(false), "generate source data")
    ("obj-size", po::value<size_t>(&obj_size)->default_value(1), "size of each object")
  ;

  po::options_description all_opts("Allowed options");
  all_opts.add(gen_opts).add(datagen_opts).add(copy_opts);

  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, all_opts), vm);

  if (vm.count("help")) {
    std::cout << all_opts << std::endl;
    return 1;
  }

  po::notify(vm);

  if (copy_client && copy_server) {
    std::cerr << "copy modes are exclusive" << std::endl;
    exit(1);
  }

  bool copy_mode = copy_client || copy_server;

  if (copy_mode || gendata) {
    if (num_objs <= 0) {
      std::cerr << "--num-objs required" << std::endl;
      exit(1);
    }
  }

  if (!copy_mode && !gendata) {
    std::cerr << "no workload mode specified" << std::endl;
    exit(1);
  }

  // connect to rados
  librados::Rados cluster;
  cluster.init(NULL);
  cluster.conf_read_file(NULL);
  int ret = cluster.connect();
  assert(ret == 0);

  // open pool i/o context
  librados::IoCtx ioctx;
  ret = cluster.ioctx_create(pool.c_str(), ioctx);
  assert(ret == 0);

  if (gendata) {
    do_gendata(ioctx, num_objs, obj_size);
  } else if (copy_client) {
    do_naive_copy(ioctx, qdepth);
  } else if (copy_server) {
    do_server_copy(ioctx, qdepth);
  }

  ioctx.close();
  cluster.shutdown();

  return 0;
}
