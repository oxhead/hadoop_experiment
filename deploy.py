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

def deploy(user):
	parameter = [
		'hadoop.runtime.dir=/home/%s/hadoop_runtime' % user,
		'yarn.scheduler.minimum-allocation-mb=512',
		'io.file.buffer.size=4096',
	] 
	generate_configuration.generate("conf", "myconf", parameter)

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

	args = parser.parse_args()
	deploy(args.user)

if __name__ == "__main__":
        main(sys.argv[1:])
