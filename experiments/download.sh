#! /bin/bash

i=0
cat ./hosts.txt | while read machine
do
  if [ $i == 10 ]
  then
    echo "download experimental results on machine ${machine}"
    rsync -rtuv root@${machine}:~/MorphDAG/experiments/ ./
    ssh -n root@${machine} "rm -rf ~/MorphDAG/experiments/*"
  fi
  i=$((i+1))
done
