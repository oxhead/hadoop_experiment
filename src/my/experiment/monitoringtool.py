#!/usr/bin/python

import sys
import os
import datetime
import time

from my.util import command
from my.experiment import helper


class Monitor():

	def __init__(self, cluster, output_dir):
		self.cluster = cluster
		self.output_dir = output_dir
		self.output_path = "/tmp/dstat_%s" % helper.get_timestamp()

	@property
	def logger(self):
		name = '.'.join([__name__, self.__class__.__name__])
		return logging.getLogger(name)

	def kill_service(self):
		cmd = "ps aux | grep dstat | grep python | tr -s ' ' | cut -d' ' -f2 | xargs kill -9"
		for node in self.cluster.getNodes():
			command.execute_remote(self.cluster.getUser(), node.host, cmd)

	def start(self):
		cmd = "nohup nice --5 dstat -tcly -mg --vm -dr -n --tcp --float --output %s > /dev/null 2>&1 &" % self.output_path
		self.kill_service()
		for node in self.cluster.getNodes():
			command.execute_remote(self.cluster.getUser(), node.host, cmd)

	def stop(self):
		self.kill_service()
		command.execute("mkdir -p %s" % self.output_dir)
		for node in self.cluster.getNodes():
                        node_type = ''
                        if self.cluster.isResourceManager(node):
                            node_type = '%s_resourcemanager' % node_type
                        if self.cluster.isNameNode(node):
                            node_type = '%s_namenode' % node_type
                        if self.cluster.isNodeManager(node):
                            node_type = '%s_mapreduce' % node_type
                        if self.cluster.isDataNode(node):
                            node_type = '%s_datanode' % node_type
                        node_output_file = "%s/dstat_%s%s.csv" % (self.output_dir, node.host, node_type)
			remote_copy_cmd = "scp %s@%s:%s %s" % (self.cluster.getUser(), node.host, self.output_path, node_output_file)
			command.execute(remote_copy_cmd)
