#!/bin/bash

mysql -uroot -h127.0.0.1 -P 29292 -e "drop database benchmark"
ghostferry-copydb conf.json &
ghostferry_pid=$!
echo GhostferryPID=$ghostferry_pid
sudo offcputime-bpfcc -df -p $ghostferry_pid 30 > out.stacks
grep github out.stacks | ./FlameGraph/flamegraph.pl --color=io --countname=us --title="$1" > out.svg

kill $ghostferry_pid
wait $ghostferry_pid

