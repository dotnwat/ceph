#!/bin/bash

set -x
set -e

bench=$1
pool=zlog
esize=1024
runtime=300
relax=120
qdepths="25"
stripesizes="50"
batchsizes="10"
outliers="0 10 100 1000 10000 100000"

ts=$(date +%s)
for qdepth in $qdepths; do
  for nobjs in $stripesizes; do
    for batchsize in $batchsizes; do
      for outlier in $outliers; do
        basefn="pd-batch-${ts}-qd_${qdepth}-no_${nobjs}-bs_${batchsize}-ol_${outlier}"

        sleep $relax

        sudo $bench --pool $pool --esize $esize --qdepth $qdepth \
          --nobjs $nobjs --batchsize $batchsize --runtime $runtime \
          --opname simple --outlier $outlier --outfile ${basefn}.simple.log

        sleep $relax

        sudo $bench --pool $pool --esize $esize --qdepth $qdepth \
          --nobjs $nobjs --batchsize $batchsize --runtime $runtime \
          --opname batch --outlier $outlier --outfile ${basefn}.batch.log

        sleep $relax

        sudo $bench --pool $pool --esize $esize --qdepth $qdepth \
          --nobjs $nobjs --batchsize $batchsize --runtime $runtime \
          --opname batch_oident --outlier $outlier --outfile ${basefn}.batch_oident.log
      done
    done
  done
done
