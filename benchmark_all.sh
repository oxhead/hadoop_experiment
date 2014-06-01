#!/bin/bash


function run() {
	storage_type=$1
	job_size=$2
	replication=$3
	echo "Type=$storage_type, Size=$job_size, Replication=$replication"
python script/benchmark.py --directory results/${storage_type}_${job_size}_${replication}replication --job terasort --size ${job_size} --node setting/node_config_${storage_type}.py -p dfs.replication=${replication}
	
}

for (( i=1; i<=128; i=i*2 )); do
	for (( j=1; j<=3; j++ )); do
		run hdd ${i}GB $j
	done
done

for (( i=1; i<=128; i=i*2 )); do
        for (( j=1; j<=3; j++ )); do
                run ssd ${i}GB $j
        done
done
