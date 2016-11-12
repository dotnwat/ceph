#!/bin/bash
set -e
set -x

# params
driver=$1
pool=rbd
num_objs=100
objsize_mb=1
repeat=3
qdepths="1 2 4 8 16 32 64"

# computed
objsize_bytes=$(($objsize_mb * 1048576))

# generate input data set
echo "make source $num_objs objs * $objsize_mb/$objsize_bytes"
$driver --pool $pool --gendata --num-objs ${num_objs} \
    --obj-size ${objsize_bytes}

for qdepth in $qdepths; do
  for rep in `seq 1 $repeat`; do
    stats_fn="run-${rep}.qd-${qdepth}"
    stats_fn="${stats_fn}.no-${num_objs}"
    stats_fn="${stats_fn}.os-${objsize_bytes}"

    $driver --pool $pool --copy-client --qdepth $qdepth \
      --num-objs ${num_objs} --obj-size ${objsize_bytes} \
      --stats-fn ${stats_fn}.client.csv

    $driver --pool $pool --copy-server --qdepth $qdepth \
      --num-objs ${num_objs} --obj-size ${objsize_bytes} \
      --stats-fn ${stats_fn}.server.csv
  done
done
