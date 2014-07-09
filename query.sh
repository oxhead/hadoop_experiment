#!/bin/bash

function parse() {
    path=$1
    storage_type=${path:8:3}
    num_devices=${path:12:1}
    replication=${path:20:1}
    job=`echo $path | cut -d"_" -f4`
    x=`echo $path | cut -d'_' -f5`
    job_size=`echo ${x} | cut -d'/' -f1`
    total_elaspe_time=`cat $path | cut -d',' -f5 | tail -n 1`
    map_mean_time=`cat $path | cut -d',' -f5 | tail -n 1`
    map_std_time=`cat $path | cut -d',' -f6 | tail -n 1`
    reduce_mean_time=`cat $path | cut -d',' -f10 | tail -n 1`
    reduce_std_time=`cat $path | cut -d',' -f11 | tail -n 1`

    echo $path ${total_elaspe_time}
}

for d in $1;
do
    path=$d/job_analysis.csv
    if [[ ! -f $path ]]; then
        echo "Failed Test: $path"
    else
        elapse=`cut -d',' -f5 $path | tail -n 1`
        re="^[0-9]+([.][0-9]+)?$"
        if [[ $elapse =~ $re ]]; then
            #echo "Successful Test: $path"
            parse $path
        else
            echo "Cannot retrieve data"
        fi
    fi
done
