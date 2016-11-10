#include <iostream>
#include <sstream>
#include <fstream>
#include <boost/program_options.hpp>
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

int main(int argc, char **argv)
{
  size_t num_objs;
  size_t obj_size;
  std::string pool;
  bool gendata;

  po::options_description gen_opts("General options");
  gen_opts.add_options()
    ("help,h", "show help message")
    ("pool", po::value<std::string>(&pool)->required(), "rados pool")
  ;

  po::options_description datagen_opts("Source data generator options");
  datagen_opts.add_options()
    ("gendata", po::bool_switch(&gendata)->default_value(false), "generate source data")
    ("num-objs", po::value<size_t>(&num_objs)->default_value(1), "number of objects")
    ("obj-size", po::value<size_t>(&obj_size)->default_value(1), "size of each object")
  ;

  po::options_description all_opts("Allowed options");
  all_opts.add(gen_opts).add(datagen_opts);

  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, all_opts), vm);

  if (vm.count("help")) {
    std::cout << all_opts << std::endl;
    return 1;
  }

  po::notify(vm);

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
  }

  return 0;
}
