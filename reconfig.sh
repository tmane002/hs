for i in {1..100}
do
    sudo killall hotstuff-app; sudo nohup ./examples/hotstuff-app --conf hotstuff.gen-sec17.conf >../nohup.out 2>&1 &; 
done

