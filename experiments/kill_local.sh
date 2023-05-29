#! /bin/bash

echo "kill all server processes"
killall start_server 2> /dev/null

echo "kill all client processes"
killall start_client 2> /dev/null
