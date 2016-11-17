#!/bin/bash

set -x
set -e

bench=$1
pool=zlog
esize=1024
runtime=300
relax=60
qdepths="10 20 40"
stripesizes="20 40 80"
batchsizes="1 5 10"

ts=$(date +%s)
for qdepth in $qdepths; do
  for nobjs in $stripesizes; do
    for batchsize in $batchsizes; do
      basefn="pd-batch-${ts}-qd_${qdepth}-no_${nobjs}-bs_${batchsize}"

      sudo $bench --pool $pool --esize $esize --qdepth $qdepth \
        --nobjs $nobjs --batchsize $batchsize --runtime $runtime \
        --opname simple --outfile ${basefn}.simple.log

      sudo $bench --pool $pool --esize $esize --qdepth $qdepth \
        --nobjs $nobjs --batchsize $batchsize --runtime $runtime \
        --opname batch --outfile ${basefn}.batch.log

      sleep $relax
    done
  done
done
