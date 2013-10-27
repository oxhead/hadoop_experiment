#!/bin/bash

function usage
{
    echo "usage: batch [[[-n jobs ] [-s sizes]] | [-h]]"
}

function process_log
{
	log_dir=$1
	grep -irn completed ${log_dir} | cut -d' ' -f6 | xargs -L 1 echo
	grep -irn completed ${log_dir} | cut -d' ' -f6 | xargs -L 1 python script/export_task_info.py -j
}

###### main

while [ "$1" != "" ]; do
    case $1 in
        -d | --dirctory )       shift
                                log_dir=$1
                                ;;
        -h | --help )           usage
                                exit
                                ;;
        * )                     usage
                                exit 1
    esac
    shift
done

process_log ${log_dir}
