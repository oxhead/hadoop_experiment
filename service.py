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
from topology import *
import mycluster

def service_action(user, service, action):
	cluster = mycluster.load()
	hadoop_dir = "~/hadoop"
	conf_dir = "%s/conf" % hadoop_dir
	dameon_script = "%s/sbin/hadoop-daemon.sh" % hadoop_dir
	yarn_script = "%s/bin/yarn" % hadoop_dir
	mapreduce_script = "%s/bin/mapred" % hadoop_dir
	hdfs_script = "%s/bin/hdfs" % hadoop_dir
	
	if service == "all":
		service_list = ["hdfs", "mapreduce", "historyserver"] if action == "start" else ["historyserver", "mapreduce", "hdfs"]
		for service in service_list:
			service_action(user, service, action)
		return
	elif service == "mapreduce":
		if action == "start":
			# start/stop ResourceManager
			execute_command("ssh %s@%s %s --config %s --script %s %s resourcemanager" % (user, cluster.mapreduce.getResourceManager().host, dameon_script, conf_dir, yarn_script, action) )
		# start/stop NodeManager
		for node in cluster.mapreduce.getNodeManagers():
			execute_command("ssh %s@%s %s --config %s --script %s %s nodemanager" % (user, node.host, dameon_script, conf_dir, yarn_script, action) )
		if action == "stop":
			execute_command("ssh %s@%s %s --config %s --script %s %s resourcemanager" % (user, cluster.mapreduce.getResourceManager().host, dameon_script, conf_dir, yarn_script, action) )
	elif service == "hdfs":
		if action == "format":
			execute_command("ssh %s@%s %s --config %s namenode -format" % (user, cluster.hdfs.getNameNode().host, hdfs_script, conf_dir) )
			for node in cluster.hdfs.getDataNodes():
				execute_command("ssh %s@%s rm -rf ~/hadoop_runtime/hdfs/datanode/*" % (user, node.host) )
		else:
			if action == "start":
				# start/stop NameNode
				execute_command("ssh %s@%s %s --config %s --script %s %s namenode" % (user, cluster.hdfs.getNameNode().host, dameon_script, conf_dir, hdfs_script, action) )
                	# start/stop DataNode
                	for node in cluster.hdfs.getDataNodes():
				execute_command("ssh %s@%s %s --config %s --script %s %s datanode" % (user, node.host, dameon_script, conf_dir, hdfs_script, action) )
	
			if action == "stop":
				execute_command("ssh %s@%s %s --config %s --script %s %s namenode" % (user, cluster.hdfs.getNameNode().
host, dameon_script, conf_dir, hdfs_script, action) )
	elif service == "historyserver":
                execute_command("ssh %s@%s %s --config %s --script %s %s historyserver" % (user, cluster.mapreduce.getResourceManager().host, dameon_script, conf_dir, mapreduce_script, action) )
                for node in cluster.mapreduce.getNodeManagers():
			execute_command("ssh %s@%s %s --config %s --script %s %s historyserver" % (user, node.host, dameon_script, conf_dir, mapreduce_script, action) )

def main(argv):
	parser = argparse.ArgumentParser(description='Configuration generator')
	parser.add_argument("-u", "--user", required=True, help="The user to deploy")
	parser.add_argument('action', choices=['start', 'stop', 'format'])
	parser.add_argument('service', choices=['mapreduce', 'hdfs', 'historyserver', 'all'])

	args = parser.parse_args()
	service_action(args.user, args.service, args.action)

if __name__ == "__main__":
        main(sys.argv[1:])
