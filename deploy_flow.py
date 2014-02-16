#!/usr/bash

dest_dir="myconf"
host_resource_manager="power6.csc.ncsu.edu"
host_name_node="power6.csc.ncsu.edu"
host_node_manager[0]="power2.csc.ncsu.edu"
host_node_manager[1]="power3.csc.ncsu.edu"
host_node_manager[2]="power4.csc.ncsu.edu"
host_node_manager[3]="power5.csc.ncsu.edu"
host_data_node[0]="power6.csc.ncsu.edu"

python2.7 generate_configuration.py --conf conf --directory $dest_dir \
	-p fs.defaultFS="hdfs://$host_name_node:10070/" \
	-p hadoop.runtime.dir="/home/chsu6/hadoop_runtime" \
	-p yarn.resourcemanager.hostname="$host_resource_manager"


for host in ${host_node_manager[@]}; do
	echo $host >> $dest_dir/slaves 
done

