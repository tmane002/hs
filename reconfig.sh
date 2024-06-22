#!/bin/bash

for i in $(seq 1 500)
do
    sudo killall hotstuff-app
    sleep 0.6
    sudo nohup ./examples/hotstuff-app --conf hotstuff.gen-sec17.conf >../nohup.out 2>&1 &
    echo $i
done

