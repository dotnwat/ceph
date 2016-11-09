#!/bin/bash

set -x
set -e

bench=$1
pool=zlog
esize=1024
qdepth=10
runtime=10
nobjs=20

# rr

numops=$(${bench} --pool $pool --opnum 0 --nops)

opnum=0
while [ $opnum -lt $numops ]; do
  outfile="tp.${opnum}.${esize}.${qdepth}.${runtime}.log"

  ${bench} --pool $pool --opnum $opnum --esize $esize --nobjs $nobjs \
    --qdepth $qdepth --runtime $runtime --outfile $outfile

  echo $opnum
  opnum=$((opnum+1))
done
