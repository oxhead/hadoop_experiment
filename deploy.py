#!/usr/bin/python

import sys
import os
import argparse
from time import sleep
import tempfile
import datetime
import time
import generate_configuration
from command import *
import mycluster

def deploy(user, parameters=None):
	default_parameters = [
		'hadoop.runtime.dir=/home/%s/hadoop_runtime' % user,
		'yarn.scheduler.minimum-allocation-mb=512',
		'io.file.buffer.size=4096',
		'yarn.scheduler.flow.assignment.model=Flow'
	] 
	if parameters is not None:
		for str in parameters:
			default_parameters.extend(parameters)

	print default_parameters
	generate_configuration.generate("conf", "myconf", default_parameters)

	cluster = mycluster.load()
        mapreduce = cluster.mapreduce
        hdfs = cluster.hdfs

        node_list = []
        node_list.append(mapreduce.getResourceManager())
        node_list.extend(mapreduce.getNodeManagers())
        node_list.append(hdfs.getNameNode())
        node_list.extend(hdfs.getDataNodes())

	for node in node_list:
		#execute_command("ssh %s mkdir -p ~/hadoop/conf && rm -rf ~/hadoop/conf/*" % node.host)
		execute_command("ssh %s mkdir -p ~/hadoop/conf" % node.host)
		execute_command("scp -r myconf/%s/* %s:~/hadoop/conf" % (node.host, node.host))

def main(argv):
	parser = argparse.ArgumentParser(description='Configuration generator')
        parser.add_argument("-u", "--user", required=True, help="The user to deploy")
	parser.add_argument("-p", "--parameter", action="append", help="The parameters of configurations")

	args = parser.parse_args()
	deploy(args.user, args.parameter)

if __name__ == "__main__":
        main(sys.argv[1:])
