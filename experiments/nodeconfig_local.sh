#! /bin/bash

# obtain host keys
for((i=0;i<$1+1;i++))
do
  if [ $i == 0 ]
    then
      id=10000
      p2p=8520
      export NODE_ID=${id}; ../launch/start_server --p2pport ${p2p} --config=true &
      continue
  fi

  p2p=$((9520+i))
  id=$((i-1))
  export NODE_ID=${id}; ../launch/start_server --p2pport ${p2p} --config=true &
done

sleep 5

# generate node files
../launch/node_config -n=$1 -le=true

