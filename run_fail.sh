#!/bin/bash

function parse() {
    path=$1
    storage_type=${path:8:3}
    num_devices=${path:12:1}
    replication=${path:20:1}
    job=`echo $path | cut -d"_" -f4`
    x=`echo $path | cut -d'_' -f5`
    job_size=`echo ${x} | cut -d'/' -f1`

    echo ==============================================================
    echo "Type=$storage_type, Devices=$num_devices, Job=$job, Size=$job_size, Replication=$replication"
    echo ==============================================================
    #python script/benchmark.py --directory results/${storage_type}_${num_devices}device_${replication}replication_${job}_${job_size} --job ${job} --size ${job_size} --node setting/node_config_${storage_type}_${num_devices}dir.py -p dfs.replication=${replication}
}

for d in results/*;
do
    path=$d/job_analysis.csv
    if [[ ! -f $path ]]; then
        #echo "Failed Test: $path"
        parse $path
    else
        elapse=`cut -d',' -f5 $path | tail -n 1`
        re="^[0-9]+([.][0-9]+)?$"
        if [[ $elapse =~ $re ]]; then
            #echo "Successful Test: $path"
            continue
        else
            parse $path
	fi
    fi
done
