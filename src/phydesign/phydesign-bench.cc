#include <boost/program_options.hpp>
#include "include/types.h"
#include "include/rados/librados.hpp"
#include "cls/phydesign/cls_phydesign.h"

namespace po = boost::program_options;

#define checkret(r,v) do { \
  if (r != v) { \
    fprintf(stderr, "error %d/%s\n", r, strerror(-r)); \
    assert(0); \
    exit(1); \
  } } while (0)

int main(int argc, char **argv)
{
  std::string pool;

  po::options_description gen_opts("General options");
  gen_opts.add_options()
    ("help,h", "show help message")
    ("pool,p", po::value<std::string>(&pool)->required(), "Pool")
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

  ceph::bufferlist inbl, outbl;

  std::vector<int> ops;
  uint64_t position = 0;

  ops.clear();
  ops.push_back(INIT_STATE);
  ::encode(ops, inbl);
  ::encode(position, inbl);
  ret = ioctx.exec("foo", "phydesign", "zlog", inbl, outbl);
  checkret(ret, 0);

  ops.clear();
  ops.push_back(READ_EPOCH_OMAP);
  inbl.clear();
  ::encode(ops, inbl);
  ::encode(position, inbl);
  ret = ioctx.exec("foo", "phydesign", "zlog", inbl, outbl);
  checkret(ret, 0);

  ops.clear();
  ops.push_back(READ_EPOCH_OMAP);
  ops.push_back(READ_EPOCH_OMAP_HDR);
  ops.push_back(READ_EPOCH_XATTR);
  ops.push_back(READ_EPOCH_NOOP);
  ops.push_back(READ_EPOCH_HEADER);
  inbl.clear();
  ::encode(ops, inbl);
  ::encode(position, inbl);
  ret = ioctx.exec("foo", "phydesign", "zlog", inbl, outbl);
  checkret(ret, 0);

  ioctx.close();
  cluster.shutdown();

  return 0;
}
