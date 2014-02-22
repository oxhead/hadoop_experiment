#!/usr/bin/python

import sys
import os
import argparse
from time import sleep
import tempfile
import datetime
import time
from command import *
import mycluster
from node_configuration import memory as memory_config
from node_configuration import slot_size as slot_size_config
from node_configuration import jdk_dir as jdk_dir_config


configurations = {
	"io.file.buffer.size": "65536",
	"yarn.nodemanager.resource.memory-mb": "66000",
	"fs.defaultFS": "file:///nfs_power2/",
}

def update_configurations(parameter_list):
	for p in parameter_list:
		p_split = p.split("=")
		configurations[p_split[0]] = p_split[1]

def update_configuration(key, value):
	configurations[key] = value

def generate(conf_dir, output_dir, parameter_list):
	cluster = mycluster.load()
	mapreduce = cluster.mapreduce 
	hdfs = cluster.hdfs

	# set up YARN server
	update_configuration('yarn.resourcemanager.hostname', mapreduce.getResourceManager().host)
	# set up HDFS server, the ending slash is required
	update_configuration('fs.defaultFS', 'hdfs://%s' % hdfs.getNameNode().host)
	# misc
	update_configuration('yarn.resourcemanager.scheduler.class', 'org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler')
	
	update_configuration('mapreduce.job.reduces', '16')

	update_configuration('yarn.inmemory.enabled', 'false')
	update_configuration('yarn.inmemory.prefetch.dir', '/dev/null')
	update_configuration('yarn.inmemory.prefetch.window', '0')
	update_configuration('yarn.inmemory.prefetch.concurrency', '0')
	update_configuration('yarn.inmemory.prefetch.tasks', '0')
	update_configuration('yarn.inmemory.prefetch.transfer', 'false')
	update_configuration('yarn.inmemory.prefetch.inputstream.enabled', 'false')	

	# update manually setting
	update_configurations(parameter_list)
	execute_command("mkdir -p %s" % output_dir)

	node_list = []
	node_list.append(mapreduce.getResourceManager())
	node_list.extend(mapreduce.getNodeManagers())
	node_list.append(hdfs.getNameNode())
	node_list.extend(hdfs.getDataNodes())

	for node in node_list:
		# create directory
		node_dir = os.path.join(output_dir, node.host)
		execute_command("mkdir -p %s" % node_dir)
		
		for f in os.listdir(conf_dir):
			file_in_path = os.path.join(conf_dir, f)
			file_out_path = os.path.join(node_dir, f)
			update_configuration('yarn.nodemanager.resource.memory-mb', memory_config[node.host])
			update_configuration('yarn.scheduler.minimum-allocation-mb', slot_size_config[node.host])
			update_configuration('JAVA_HOME', jdk_dir_config[node.host])
			if "xml" in f or "sh" in f:
				with open(file_in_path, "r") as fp_in:
					content = fp_in.read()
					for (key, value) in configurations.items():
						content = content.replace("${%s}" % key, value)
					with open(file_out_path, "w") as fp_out:
						fp_out.write(content)
			else:
				execute_command("cp %s %s" % (file_in_path, file_out_path))

def main(argv):

	parser = argparse.ArgumentParser(description='Configuration generator')

	parser.add_argument("-c", "--conf", required=True, help="The directory for the Hadoop configuration template")
	parser.add_argument("-d", "--directory", required=True, help="The output directory")
	parser.add_argument("-p", "--parameter", action="append", default=[], help="The format should be p=x")

        args = parser.parse_args()

	generate(args.conf, args.directory, args.parameter)

if __name__ == "__main__":
        main(sys.argv[1:])
