#!/bin/bash


function run() {
	storage_type=$1
	num_devices=$2
	replication=$3
	job_size=$4
	job=$5
	echo ==============================================================
	echo "Type=$storage_type, Job=$job, Size=$job_size, Replication=$replication"
	echo ==============================================================
	mkdir -p results/${storage_type}_${num_devices}device_${replication}replication_${job}_${job_size}
	#python script/benchmark.py --directory results/${storage_type}_${num_devices}device_${replication}replication_${job}_${job_size} --job ${job} --size ${job_size} --node setting/node_config_${storage_type}_${num_devices}dir.py -p dfs.replication=${replication}
}

# job size
for (( i=1; i<=128; i=i*2 )); do
	# replication
	for (( j=1; j<=3; j++ )); do
		# num_devices
		for (( k=1; k<=3; k++ )); do
			#run hdd $k ${j} ${i}GB terasort
			#run hdd $k ${j} ${i}GB wordcount
			#run hdd $k ${j} ${i}GB grep
			continue
		done
	done
done


for (( i=1; i<=128; i=i*2 )); do
        for (( j=1; j<=3; j++ )); do
                run ssd 1 $j ${i}GB terasort
		run ssd 1 $j ${i}GB wordcount
		run ssd 1 $j ${i}GB grep
        done
done
