#!/usr/bin/python

import sys
import os
import argparse
from time import sleep
import tempfile
import datetime
import time

from command import *

class Experiment(object):

	def __init__(self, plan, hadoop, ganglia):
		self.plan = plan
		self.hadoop = hadoop
		self.gandlia = ganglia
	

	def run(self):
		setup()
		submit()
		complete()
		collect_data()
	
	def submit(self):
		self.plan.run(hadoop)

	def setup(self):
		#creat dir first
		self.hadoop.starT()

	def complete(self):
		hadoop.stop()
		
	def collect_data(self):
		self.gangalia.collect_data()

class Plan(object):
	def __init__(self, hadoop):
		self.hadoop = hadoop
	def run(self):
		print 'empy queue'


class Node(object):
	def __init__(self, host, user=None):
		self.host = host
		self.host = user
	def __repr__(self):
		return "%s@%s" % (self.host, self.user)
	def execute_commnad(self, cmd):
		execute_remote_command(self, cmd)

class Hadoop(object):
	def __init__(self, host, dir, decoupled=True):
		self.manager = Node(host)
		self.dir = dir

	def start(self):
		if not decoupled:
			self.manager.execute_remote_command("%s/sbin/start-dfs.sh" % self.dir)
		self.manager.execute_remote_commnad("%s/sbin/start-yarn.sh" % self.dir)	
		return True

	def stop(self):
		if not decoupled:
			self.manager.execute_remote_command("%s/sbin/stop-dfs.sh" % self.dir)
		self.manager.execute_remote_command("%s/sbin/stop-yarn.sh" % self.dir)
		return True

class Ganglia(object):

	collect_metric_table = {
                "cpu": ["cpu_user", "cpu_system", "cpu_wio", "cpu_idle", "cpu_aidle", "cpu_nice"],
                "memory": ["mem_free", "mem_cached", "mem_buffers", "mem_shared"],
                "network": ["bytes_in", "bytes_out"],
        }
	
	def __init__(self, node, rrds_dir=""):
		self.node = node
		self.rrds_dir = rrds_dir
	
	def collect_data(self, cluster, nodes, dest):
		remote_tmp_dir = os.path.join("/tmp", "rrds_%s_%s" % (cluster, int(time.time())))
        	collect_step = 5
        	for node in nodes:
                	remote_node_dir = os.path.join(remote_tmp_dir, node)
                	remote_command(ganglia_host, "mkdir -p %s" % remote_node_dir)
                	for metric in collect_metric_list:
                        	remote_output_file = os.path.join(remote_node_dir, metric)
                        	rrd_list = ["DEF:%s=%s/%s/%s/%s.rrd:sum:AVERAGE XPORT:%s:'%s'" % (rrd_metric, rrds_dir, cluster_name, node, rrd_metric, rrd_metric, rrd_metric) for rrd_metric in collect_metric_table[metric]]
                        	rrd_list_string = " ".join(rrd_list)
                        	cmd = "rrdtool xport --start %s --end %s %s --step %s > %s" % (time_start, time_end, rrd_list_string, collect_step, remote_output_file)
                        	remote_command(ganglia_host, cmd)
        	remote_copy(ganglia_host, remote_tmp_dir, output_dir)	
		
