#! /bin/bash

# i=0
cat ./hosts.txt | while read machine
do
  echo "kill servers on machine ${machine}"
  ssh -n root@${machine} "killall start_server 2> /dev/null" &
  # if [[ $i == 0 || $i == 1 ]]
  #   then
  #   echo "kill client on machine ${machine}"
  #   ssh -n root@${machine} "killall start_client 2> /dev/null"
  # fi
  # i=$((i+1))
done
