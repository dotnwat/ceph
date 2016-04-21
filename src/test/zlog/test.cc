#include <atomic>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <iostream>
#include <boost/program_options.hpp>
#include "include/rados/librados.hpp"

namespace po = boost::program_options;

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

static std::mutex io_lock;
static std::condition_variable io_cond;
static std::atomic_ullong outstanding_ios;
static std::atomic_ullong ios_completed;

struct aio_state {
  librados::AioCompletion *rc;
  uint64_t submitted_ns;
};

static void handle_io_cb(librados::completion_t cb, void *arg)
{
  aio_state *io = (aio_state*)arg;

  // clean-up
  io_lock.lock();
  outstanding_ios--;
  io_lock.unlock();

  assert(io->rc->get_return_value() == 0);
  io->rc->release();
  io_cond.notify_one();

  ios_completed++;

  delete io;
}

class IOGen {
 public:
  IOGen(int qdepth) :
    qdepth_(qdepth)
  {}

  virtual ~IOGen() {}

  void run() {
    outstanding_ios = 0;
    std::unique_lock<std::mutex> lock(io_lock);
    for (;;) {
      while (outstanding_ios < (unsigned)qdepth_) {
        // create context to track the io
        aio_state *io = new aio_state;
        io->rc = librados::Rados::aio_create_completion();
        io->rc->set_complete_callback(io, handle_io_cb);
        assert(io->rc);

        // create operation
        gen_op(io->rc, &io->submitted_ns);

        outstanding_ios++;
      }

      io_cond.wait(lock, [&]{ return outstanding_ios < (unsigned)qdepth_; });
    }
  }

  virtual void gen_op(librados::AioCompletion *rc, uint64_t *submitted_ns) = 0;

 private:
  int qdepth_;
};

class LibRadosIOGen : public IOGen {
 public:
  LibRadosIOGen(int qdepth, librados::IoCtx *ioctx) :
    IOGen(qdepth), ioctx_(ioctx)
  {
    bufferlist bl;
    char buf[4096];
    bl.append(buf, sizeof(buf));
    int ret = ioctx->create("oid", false);
    assert(ret == 0);
    ret = ioctx->write_full("oid", bl);
    assert(ret == 0);
  }

  virtual void gen_op(librados::AioCompletion *rc, uint64_t *submitted_ns) {
    *submitted_ns = getns();
    bufferlist bl;
    char buf[100];
    bl.append(buf, sizeof(buf));
#if 0
    int ret = ioctx_->aio_append(
        "oid", rc, bl, bl.length());
#else
    int ret = ioctx_->aio_zlog_append_check_epoch_header(
        "oid", rc, 100, bl, bl.length());
#endif
    assert(ret == 0);
  }

 private:
  librados::IoCtx *ioctx_;
};

static void report(int period)
{
  uint64_t expr_start_ns = getns();

  ios_completed = 0;
  uint64_t start_ns = getns();

  while (1) {
    // length of time to accumulate iops stats
    sleep(period);

    uint64_t period_ios_completed = ios_completed.exchange(0);
    uint64_t end_ns = getns();
    uint64_t dur_ns = end_ns - start_ns;
    start_ns = end_ns;

    // completed ios in prev period
    double iops = (double)(period_ios_completed * 1000000000ULL) / (double)dur_ns;

    uint64_t elapsed_sec = (end_ns - expr_start_ns) / 1000000000ULL;
    std::cout << elapsed_sec << "s: " << "rate=" << (int)iops << " iops" << std::endl;
  }
}

int main(int argc, char **argv)
{
  std::string pool;
  std::string mode;
  int report_period;
  int qdepth;

  po::options_description desc("Allowed options");
  desc.add_options()
    ("pool", po::value<std::string>(&pool)->default_value("rbd"), "Pool name")
    ("mode", po::value<std::string>(&mode), "Mode")
    ("qdepth", po::value<int>(&qdepth)->default_value(1), "Queue depth")
    ("report", po::value<int>(&report_period)->default_value(5), "Report sec")
  ;

  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);
  po::notify(vm);

  assert(mode == "librados" || mode == "cls");

  // connect to rados
  librados::Rados cluster;
  cluster.init(NULL);
  cluster.conf_read_file(NULL);
  int ret = cluster.connect();
  assert(ret == 0);

  // open target pool i/o context
  librados::IoCtx ioctx;
  ret = cluster.ioctx_create(pool.c_str(), ioctx);
  assert(ret == 0);

  IOGen *gen;
  if (mode == "librados") {
    gen = new LibRadosIOGen(qdepth, &ioctx);
  } else if (mode == "cls") {
    //gen = new ClsIOGen(qdepth, &ioctx);
    assert(0);
  } else
    assert(0);

  std::thread runner{[&] {
    gen->run();
  }};

  report(report_period);

  return 0;
}
