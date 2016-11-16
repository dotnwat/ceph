#include <boost/program_options.hpp>
#include "include/types.h"
#include "include/rados/librados.hpp"
#include "cls/phydesign/cls_phydesign.h"
#include <iostream>
#include <thread>
#include <signal.h>
#include <fstream>
#include <mutex>
#include <condition_variable>
#include <atomic>

namespace po = boost::program_options;

#define checkret(r,v) do { \
  if (r != v) { \
    fprintf(stderr, "error %d/%s\n", r, strerror(-r)); \
    assert(0); \
    exit(1); \
  } } while (0)

static inline uint64_t __getns(clockid_t clock)
{
  struct timespec ts;
  int ret = clock_gettime(clock, &ts);
  assert(ret == 0);
  return (((uint64_t)ts.tv_sec) * 1000000000ULL) + ts.tv_nsec;
}

static inline uint64_t getns()
{
  return __getns(CLOCK_MONOTONIC);
}

static volatile int stop;
static void sigint_handler(int sig) {
  stop = 1;
}

static std::atomic<uint64_t> ios_completed;
static std::atomic<uint64_t> outstanding_ios;
static std::condition_variable io_cond;
static std::mutex io_lock;

struct aio_state {
   librados::AioCompletion *c;
};

static void handle_aio_cb(librados::completion_t cb, void *arg)
{
  aio_state *io = (aio_state*)arg;

  // notify workload to gen more ios
  io_lock.lock();
  outstanding_ios--;
  io_lock.unlock();
  io_cond.notify_one();

  if (io->c->get_return_value()) {
    std::cerr << "error in aio cb" << std::endl;
    checkret(io->c->get_return_value(), 0);
    stop = 1;
    exit(1);
  }

  ios_completed++;

  io->c->release();
  delete io;
}

static void delete_objects(librados::IoCtx *ioctx, int nobjs)
{
  assert(nobjs > 0);

  for (int i = 0; i < nobjs; i++) {
    stringstream ss;
    ss << "obj." << i;
    int ret = ioctx->remove(ss.str());
    checkret(ret, 0);
    std::cout << "removing object: " << ss.str() << " "
      << (i+1) << "/" << nobjs
      << "\r" << std::flush;
  }

  std::cout << std::endl << std::flush;
}

static void init_objects(librados::IoCtx *ioctx, int nobjs)
{
  assert(nobjs > 0);

  for (int i = 0; i < nobjs; i++) {
    stringstream ss;
    ss << "obj." << i;

    // setup op components
    std::vector<int> ops({INIT_STATE});
    uint64_t position = 0;
    ceph::bufferlist blob;

    ceph::bufferlist inbl;
    ::encode(ops, inbl);
    ::encode(position, inbl);
    ::encode(blob, inbl);

    ceph::bufferlist outbl;
    int ret = ioctx->exec(ss.str(), "phydesign", "zlog", inbl, outbl);
    checkret(ret, 0);

    std::cout << "init object: " << ss.str() << " "
      << (i+1) << "/" << nobjs
      << "\r" << std::flush;
  }

  std::cout << std::endl << std::flush;
}

static std::vector<int> make_ops_vector(int *opnums)
{
  std::vector<int> ops;
  for (int i = 1; i < 5; i++) {
    ops.push_back(opnums[i]);
  }
  return ops;
}

static void workload_func(librados::IoCtx *ioctx, int nobjs,
    const unsigned qdepth, const int entry_size, int *opnums)
{
  // create random data to use for payloads
  size_t rand_buf_size = 1ULL<<23;
  std::string rand_buf;
  rand_buf.reserve(rand_buf_size);
  std::ifstream ifs("/dev/urandom", std::ios::binary | std::ios::in);
  std::copy_n(std::istreambuf_iterator<char>(ifs),
      rand_buf_size, std::back_inserter(rand_buf));
  const char *rand_buf_raw = rand_buf.c_str();

  // grab random slices from the random bytes
  std::default_random_engine generator;
  std::uniform_int_distribution<int> rand_dist;
  rand_dist = std::uniform_int_distribution<int>(0,
      rand_buf_size - entry_size - 1);

  // ops vector describing seq of ops to run in object class
  const std::vector<int> ops = make_ops_vector(opnums);

  // prepare the objects in the stripe
  init_objects(ioctx, nobjs);

  outstanding_ios = 0;    // maintains queue depth
  uint64_t pos = 0;       // current log position

  std::unique_lock<std::mutex> lock(io_lock);
  for (;;) {
    while (outstanding_ios < qdepth) {
      // create aio context
      aio_state *io = new aio_state;
      io->c = librados::Rados::aio_create_completion(io, NULL, handle_aio_cb);
      assert(io->c);

      // create log entry from random data
      size_t buf_offset = rand_dist(generator);
      ceph::bufferlist blob;
      blob.append(rand_buf_raw + buf_offset, entry_size);

      // setup op
      ceph::bufferlist inbl;
      ::encode(ops, inbl);
      ::encode(pos, inbl);
      ::encode(blob, inbl);

      librados::ObjectWriteOperation op;
      op.exec("phydesign", "zlog", inbl);

      // generate target object name in stripe
      stringstream ss;
      ss << "obj." << (pos % nobjs);

      int ret = ioctx->aio_operate(ss.str(), io->c, &op);
      checkret(ret, 0);

      pos++;
      outstanding_ios++;
    }

    io_cond.wait(lock, [&]{ return outstanding_ios < qdepth || stop; });

    if (stop)
      break;
  }

  for (;;) {
    std::cout << "draining ios: " << outstanding_ios << " remaining" << std::endl;
    if (outstanding_ios == 0)
      break;
    sleep(1);
  }

  delete_objects(ioctx, nobjs);
}

static void report(int stats_window, const std::string tp_log_fn)
{
  // open the output stream
  int fd = -1;
  if (!tp_log_fn.empty()) {
    fd = open(tp_log_fn.c_str(), O_WRONLY|O_CREAT|O_TRUNC, 0440);
    assert(fd != -1);
  }

  // init
  ios_completed = 0;
  uint64_t window_start = getns();

  while (!stop) {
    sleep(stats_window);
    if (stop)
      break;

    // re-sample
    uint64_t ios_completed_in_window = ios_completed.exchange(0);
    uint64_t window_end = getns();

    // calc rate
    uint64_t window_dur = window_end - window_start;
    double iops = (double)(ios_completed_in_window *
        1000000000ULL) / (double)window_dur;

    time_t now = time(NULL);
    std::cout << "time " << now << " iops " << (int)iops << std::endl;

    if (fd != -1) {
      dprintf(fd, "time %llu iops %llu\n",
          (unsigned long long)now,
          (unsigned long long)iops);
    }

    // reset
    window_start = window_end;
  }

  if (fd != -1) {
    fsync(fd);
    close(fd);
  }
}

/*
 * 1: normal metadata in rocksdb and entries in bytestream
 */
static int op_combos[][5] = {
  {0, READ_EPOCH_OMAP, READ_OMAP_INDEX_ENTRY, WRITE_OMAP_INDEX_ENTRY, APPEND_DATA},
  {0, READ_EPOCH_OMAP, READ_OMAP_INDEX_ENTRY, APPEND_DATA, WRITE_OMAP_INDEX_ENTRY},
  {0, READ_EPOCH_OMAP, WRITE_OMAP_INDEX_ENTRY, READ_OMAP_INDEX_ENTRY, APPEND_DATA},
  {0, READ_EPOCH_OMAP, WRITE_OMAP_INDEX_ENTRY, APPEND_DATA, READ_OMAP_INDEX_ENTRY},
  {0, READ_EPOCH_OMAP, APPEND_DATA, READ_OMAP_INDEX_ENTRY, WRITE_OMAP_INDEX_ENTRY},
  {0, READ_EPOCH_OMAP, APPEND_DATA, WRITE_OMAP_INDEX_ENTRY, READ_OMAP_INDEX_ENTRY},
  {0, READ_OMAP_INDEX_ENTRY, READ_EPOCH_OMAP, WRITE_OMAP_INDEX_ENTRY, APPEND_DATA},
  {0, READ_OMAP_INDEX_ENTRY, READ_EPOCH_OMAP, APPEND_DATA, WRITE_OMAP_INDEX_ENTRY},
  {0, READ_OMAP_INDEX_ENTRY, WRITE_OMAP_INDEX_ENTRY, READ_EPOCH_OMAP, APPEND_DATA},
  {0, READ_OMAP_INDEX_ENTRY, WRITE_OMAP_INDEX_ENTRY, APPEND_DATA, READ_EPOCH_OMAP},
  {0, READ_OMAP_INDEX_ENTRY, APPEND_DATA, READ_EPOCH_OMAP, WRITE_OMAP_INDEX_ENTRY},
  {0, READ_OMAP_INDEX_ENTRY, APPEND_DATA, WRITE_OMAP_INDEX_ENTRY, READ_EPOCH_OMAP},
  {0, WRITE_OMAP_INDEX_ENTRY, READ_EPOCH_OMAP, READ_OMAP_INDEX_ENTRY, APPEND_DATA},
  {0, WRITE_OMAP_INDEX_ENTRY, READ_EPOCH_OMAP, APPEND_DATA, READ_OMAP_INDEX_ENTRY},
  {0, WRITE_OMAP_INDEX_ENTRY, READ_OMAP_INDEX_ENTRY, READ_EPOCH_OMAP, APPEND_DATA},
  {0, WRITE_OMAP_INDEX_ENTRY, READ_OMAP_INDEX_ENTRY, APPEND_DATA, READ_EPOCH_OMAP},
  {0, WRITE_OMAP_INDEX_ENTRY, APPEND_DATA, READ_EPOCH_OMAP, READ_OMAP_INDEX_ENTRY},
  {0, WRITE_OMAP_INDEX_ENTRY, APPEND_DATA, READ_OMAP_INDEX_ENTRY, READ_EPOCH_OMAP},
  {0, APPEND_DATA, READ_EPOCH_OMAP, READ_OMAP_INDEX_ENTRY, WRITE_OMAP_INDEX_ENTRY},
  {0, APPEND_DATA, READ_EPOCH_OMAP, WRITE_OMAP_INDEX_ENTRY, READ_OMAP_INDEX_ENTRY},
  {0, APPEND_DATA, READ_OMAP_INDEX_ENTRY, READ_EPOCH_OMAP, WRITE_OMAP_INDEX_ENTRY},
  {0, APPEND_DATA, READ_OMAP_INDEX_ENTRY, WRITE_OMAP_INDEX_ENTRY, READ_EPOCH_OMAP},
  {0, APPEND_DATA, WRITE_OMAP_INDEX_ENTRY, READ_EPOCH_OMAP, READ_OMAP_INDEX_ENTRY},
  {0, APPEND_DATA, WRITE_OMAP_INDEX_ENTRY, READ_OMAP_INDEX_ENTRY, READ_EPOCH_OMAP},
  {1, READ_EPOCH_HEADER, READ_OMAP_INDEX_ENTRY, WRITE_OMAP_INDEX_ENTRY, APPEND_DATA},
  {1, READ_EPOCH_HEADER, READ_OMAP_INDEX_ENTRY, APPEND_DATA, WRITE_OMAP_INDEX_ENTRY},
  {1, READ_EPOCH_HEADER, WRITE_OMAP_INDEX_ENTRY, READ_OMAP_INDEX_ENTRY, APPEND_DATA},
  {1, READ_EPOCH_HEADER, WRITE_OMAP_INDEX_ENTRY, APPEND_DATA, READ_OMAP_INDEX_ENTRY},
  {1, READ_EPOCH_HEADER, APPEND_DATA, READ_OMAP_INDEX_ENTRY, WRITE_OMAP_INDEX_ENTRY},
  {1, READ_EPOCH_HEADER, APPEND_DATA, WRITE_OMAP_INDEX_ENTRY, READ_OMAP_INDEX_ENTRY},
  {1, READ_OMAP_INDEX_ENTRY, READ_EPOCH_HEADER, WRITE_OMAP_INDEX_ENTRY, APPEND_DATA},
  {1, READ_OMAP_INDEX_ENTRY, READ_EPOCH_HEADER, APPEND_DATA, WRITE_OMAP_INDEX_ENTRY},
  {1, READ_OMAP_INDEX_ENTRY, WRITE_OMAP_INDEX_ENTRY, READ_EPOCH_HEADER, APPEND_DATA},
  {1, READ_OMAP_INDEX_ENTRY, WRITE_OMAP_INDEX_ENTRY, APPEND_DATA, READ_EPOCH_HEADER},
  {1, READ_OMAP_INDEX_ENTRY, APPEND_DATA, READ_EPOCH_HEADER, WRITE_OMAP_INDEX_ENTRY},
  {1, READ_OMAP_INDEX_ENTRY, APPEND_DATA, WRITE_OMAP_INDEX_ENTRY, READ_EPOCH_HEADER},
  {1, WRITE_OMAP_INDEX_ENTRY, READ_EPOCH_HEADER, READ_OMAP_INDEX_ENTRY, APPEND_DATA},
  {1, WRITE_OMAP_INDEX_ENTRY, READ_EPOCH_HEADER, APPEND_DATA, READ_OMAP_INDEX_ENTRY},
  {1, WRITE_OMAP_INDEX_ENTRY, READ_OMAP_INDEX_ENTRY, READ_EPOCH_HEADER, APPEND_DATA},
  {1, WRITE_OMAP_INDEX_ENTRY, READ_OMAP_INDEX_ENTRY, APPEND_DATA, READ_EPOCH_HEADER},
  {1, WRITE_OMAP_INDEX_ENTRY, APPEND_DATA, READ_EPOCH_HEADER, READ_OMAP_INDEX_ENTRY},
  {1, WRITE_OMAP_INDEX_ENTRY, APPEND_DATA, READ_OMAP_INDEX_ENTRY, READ_EPOCH_HEADER},
  {1, APPEND_DATA, READ_EPOCH_HEADER, READ_OMAP_INDEX_ENTRY, WRITE_OMAP_INDEX_ENTRY},
  {1, APPEND_DATA, READ_EPOCH_HEADER, WRITE_OMAP_INDEX_ENTRY, READ_OMAP_INDEX_ENTRY},
  {1, APPEND_DATA, READ_OMAP_INDEX_ENTRY, READ_EPOCH_HEADER, WRITE_OMAP_INDEX_ENTRY},
  {1, APPEND_DATA, READ_OMAP_INDEX_ENTRY, WRITE_OMAP_INDEX_ENTRY, READ_EPOCH_HEADER},
  {1, APPEND_DATA, WRITE_OMAP_INDEX_ENTRY, READ_EPOCH_HEADER, READ_OMAP_INDEX_ENTRY},
  {1, APPEND_DATA, WRITE_OMAP_INDEX_ENTRY, READ_OMAP_INDEX_ENTRY, READ_EPOCH_HEADER},
};

int main(int argc, char **argv)
{
  std::string pool;
  unsigned opnum;
  unsigned qdepth;
  unsigned esize;
  int runtime;
  std::string outfile;
  int nobjs;

  po::options_description gen_opts("General options");
  gen_opts.add_options()
    ("help,h", "show help message")
    ("opnum", po::value<unsigned>(&opnum)->required(), "op num")
    ("qdepth", po::value<unsigned>(&qdepth)->required(), "queue depth")
    ("esize", po::value<unsigned>(&esize)->required(), "entry size")
    ("runtime", po::value<int>(&runtime)->default_value(0), "runtime (sec)")
    ("pool,p", po::value<std::string>(&pool)->required(), "pool")
    ("nobjs", po::value<int>(&nobjs)->required(), "stripe size (num objects)")
    ("outfile", po::value<std::string>(&outfile)->default_value(""), "outfile")
  ;

  po::options_description all_opts("Allowed options");
  all_opts.add(gen_opts);

  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, all_opts), vm);

  if (vm.count("help")) {
    std::cout << all_opts << std::endl;
    return 1;
  }

  po::notify(vm);

  unsigned nops = (sizeof(op_combos) / sizeof((op_combos)[0]));
  assert(opnum < nops);

  signal(SIGINT, sigint_handler);
  stop = 0;

  // connect to rados
  librados::Rados cluster;
  cluster.init(NULL);
  cluster.conf_read_file(NULL);
  int ret = cluster.connect();
  checkret(ret, 0);

  // open pool i/o context
  librados::IoCtx ioctx;
  ret = cluster.ioctx_create(pool.c_str(), ioctx);
  checkret(ret, 0);

  std::thread runner(workload_func, &ioctx, nobjs,
    qdepth, esize, op_combos[opnum]);

  std::thread reporter(report, 2, outfile);

  if (runtime) {
    sleep(runtime);
    stop = 1;
  }

  runner.join();
  reporter.join();

  ioctx.close();
  cluster.shutdown();

  return 0;
}
