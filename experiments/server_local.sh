#! /bin/bash

rm -rf ../dagdata/*

sleep 1

for((i=0;i<$1+1;i++))
do
  # starts the tx sender
  if [ $i == 0 ]
    then
      rpc=9545
      p2p=8520
      id=10000
      export NODE_ID=${id}; ../launch/start_server --rpcport ${rpc} --p2pport ${p2p} --number $1 --sender=true &
      continue
  fi

  # starts MorphDAG servers
  rpc=$((6499+i))
  p2p=$((9520+i))
  id=$((i-1))
  export NODE_ID=${id}; ../launch/start_server --rpcport ${rpc} --p2pport ${p2p} --number $1 --cycles $2 &
done
