#!/bin/bash

echo "start time"
echo $(date +"%Y-%m-%d %H:%M:%S")

for i in $(seq 1 37)
do
    sudo killall hotstuff-app
    sleep 0.1
    sudo nohup ./examples/hotstuff-app --conf hotstuff.gen-sec17.conf >../nohup.out 2>&1 &
    echo $i
done

echo "end time"
echo $(date +"%Y-%m-%d %H:%M:%S")
