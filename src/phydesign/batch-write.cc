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

static void workload_func(librados::IoCtx *ioctx, int nobjs,
    const unsigned qdepth, const int entry_size, std::string opname,
    int batchsize)
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

  // prepare the objects in the stripe
  init_objects(ioctx, nobjs);

  outstanding_ios = 0;    // maintains queue depth
  uint64_t pos = 0;       // current log position

  uint64_t obj_pos[nobjs];
  for (int i = 0; i < nobjs; i++) {
    obj_pos[i] = i;
  }
  /*
   * we are starting at the first append here as a simple hack. the omap bulk
   * range scan in object class has a "start_after" parameter, and it isn't
   * immediately obvious how to generate a key that comes before pos=0. So we
   * just start here with 1 to avoid any special case.
   */
  obj_pos[0] += nobjs;

  std::unique_lock<std::mutex> lock(io_lock);
  for (;;) {
    while (outstanding_ios < qdepth) {
      // create aio context
      aio_state *io = new aio_state;
      io->c = librados::Rados::aio_create_completion(io, NULL, handle_aio_cb);
      assert(io->c);

      size_t obj_idx = pos % nobjs;

      // create log entry from random data
      std::vector<entry_info> entries;
      for (int i = 0; i < batchsize; i++) {
        size_t buf_offset = rand_dist(generator);
        ceph::bufferlist blob;
        blob.append(rand_buf_raw + buf_offset, entry_size);
        entry_info ei;
        ei.position = obj_pos[obj_idx];
        ei.data = blob;
        entries.push_back(ei);
        obj_pos[obj_idx] += nobjs;
      }

      // setup op input
      ceph::bufferlist inbl;
      uint64_t epoch = 33;
      ::encode(epoch, inbl);
      ::encode(entries, inbl);

      librados::ObjectWriteOperation op;
      op.exec("phydesign", opname.c_str(), inbl);

      // generate target object name in stripe
      stringstream ss;
      ss << "obj." << (pos % nobjs);

      int ret = ioctx->aio_operate(ss.str(), io->c, &op);
      checkret(ret, 0);

      outstanding_ios++;
      pos++;
    }

    io_cond.wait(lock, [&]{ return outstanding_ios < qdepth || stop; });

    if (stop)
      break;
  }

  for (;;) {
    std::cout << "draining ios: " << outstanding_ios << " remaining" << std::endl;
    if (outstanding_ios == 0) {
      io_lock.unlock();
      break;
    }
    io_lock.unlock();
    sleep(1);
    io_lock.lock();
  }

  delete_objects(ioctx, nobjs);
}

static void report(int stats_window, const std::string tp_log_fn,
    int batchsize, const std::string opname, int qdepth, int esize,
    int nobjs)
{
  // open the output stream
  int fd = -1;
  if (!tp_log_fn.empty()) {
    fd = open(tp_log_fn.c_str(), O_WRONLY|O_CREAT|O_TRUNC, 0440);
    assert(fd != -1);
    dprintf(fd, "time,opname,qdepth,esize,nobjs,batchsize,raw_iops,eff_iops\n");
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
    double eff_iops = iops * batchsize;

    time_t now = time(NULL);
    std::cout << "time " << now << " raw_iops " << (int)iops
      << " eff_iops " << (int)eff_iops << std::endl;

    if (fd != -1) {
      dprintf(fd, "%llu,%s,%llu,%llu,%llu,%llu,%llu,%llu\n",
          (unsigned long long)now,
          opname.c_str(),
          (unsigned long long)qdepth,
          (unsigned long long)esize,
          (unsigned long long)nobjs,
          (unsigned long long)batchsize,
          (unsigned long long)iops,
          (unsigned long long)eff_iops);
    }

    // reset
    window_start = window_end;
  }

  if (fd != -1) {
    fsync(fd);
    close(fd);
  }
}

int main(int argc, char **argv)
{
  std::string pool;
  std::string opname;
  unsigned qdepth;
  unsigned esize;
  int runtime;
  std::string outfile;
  int nobjs;
  int batchsize;

  po::options_description gen_opts("General options");
  gen_opts.add_options()
    ("help,h", "show help message")
    ("opname", po::value<std::string>(&opname)->required(), "op name")
    ("qdepth", po::value<unsigned>(&qdepth)->required(), "queue depth")
    ("esize", po::value<unsigned>(&esize)->required(), "entry size")
    ("runtime", po::value<int>(&runtime)->default_value(0), "runtime (sec)")
    ("pool,p", po::value<std::string>(&pool)->required(), "pool")
    ("nobjs", po::value<int>(&nobjs)->required(), "stripe size (num objects)")
    ("batchsize", po::value<int>(&batchsize)->required(), "append batch size")
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

  assert(!opname.empty());
  assert(qdepth > 0);
  assert(esize > 0);
  assert(runtime >= 0);
  assert(nobjs > 0);
  assert(batchsize > 0);

  if (opname == "simple") {
    opname = "zlog_batch_write_simple";
  } else if (opname == "batch") {
    opname = "zlog_batch_write_batch";
  } else {
    std::cerr << "invalid op name" << std::endl;
    exit(1);
  }

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
    qdepth, esize, opname, batchsize);

  std::thread reporter(report, 2, outfile,
      batchsize, opname, qdepth, esize, nobjs);

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
