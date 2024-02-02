#! /bin/bash

i=0
cat ./hosts.txt | while read machine
do
   echo "deploy server code on machine ${machine}"
   ssh -n root@${machine} "rm -rf ~/MorphDAG/nodefile/*"
   rsync -rtuv ./install.sh root@${machine}:~
   ssh -n root@${machine} "./install.sh"
   rsync -rtuv ../launch/start_server root@${machine}:~/MorphDAG/launch

   if [[ $i == 0 || $i == 1 ]]
   then
     echo "deploy client code on machine ${machine}"
#     rsync -rtuv ../launch/start_client root@${machine}:~/MorphDAG/launch
     rsync -rtuv ./loads.txt root@${machine}:~/MorphDAG/experiments
     rsync -rtuv ./large_loads.txt root@${machine}:~/MorphDAG/experiments
   fi

   i=$((i+1))
done