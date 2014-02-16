#!/usr/bin/python

import sys
import os
import argparse
from time import sleep
import tempfile
import datetime
import time
from command import *
import generate_configuration
import generate_topology
from topology import *
from command import Command

def load():
	import load_data
        cluster = generate_topology.generate(load_data.getMapReduceClusterConfig(), load_data.getHDFSClusterConfig())
        mapreduce = cluster.mapreduce
        hdfs = cluster.hdfs
	return cluster

def measure(size):
	cluster = load()
	hadoop_dir = "~/hadoop"
	output_dir = "/output"
	log_dir = "log"

	execute_command("mkdir -p %s" % log_dir)

	# per GB
	task_size_list = [1]

	for task_size in task_size_list:
		now = datetime.datetime.now()
		task_id = now.strftime("%Y-%m-%d_%H-%M-%S")
		task_log = "%s/%s.log" % (log_dir, task_id)
		cmd = Command("%s/bin/hadoop jar %s/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar custommap /dataset/wikipedia_1GB %s/measure_storage_%s 0 1 1 1024 > %s 2>&1" % (hadoop_dir, hadoop_dir, output_dir, task_id, task_log) )

		print cmd.command
		cmd.run()

		lookup_cmd = Command("grep 'Total time spent by all maps in occupied slots (ms)' %s | cut -d'=' -f2" % task_log)
		lookup_cmd.run()
		running_time = int(lookup_cmd.output.strip())
		print "total running time: %s sec" % (running_time/1000.0)
		print "average storage flow capability: %s MB/s" % (task_size*1024.0/(running_time/1000.0))



def main(argv):
	parser = argparse.ArgumentParser(description='Configuration generator')
	parser.add_argument("-size", "--size", required=True, help="The size varies from 1 to the specified GB")

	args = parser.parse_args()
	measure(args.size)

if __name__ == "__main__":
        main(sys.argv[1:])
