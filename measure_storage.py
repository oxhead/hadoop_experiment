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
import myjob
import myinfo
import mycluster
import mylog

def measure(size):

	cluster = mycluster.load()

	# per GB
	for job_size in range(size, size+1):
		log_list = []
		returncode_list = []
		
		for job_id in range(1, job_size+1):
			setting = myjob.get_job_setting(job="custommap", prefix="storage-flow-%s" % job_id)
			log_list.append(setting['job_log'])
			returncode_list.append(setting['job_returncode'])

			cmd = Command("%s/bin/hadoop jar %s/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar custommap -Dmapreduce.job.reduces=1 -Dmapreduce.map.memory.mb=%s %s %s 0 1 1 1024 > %s 2>&1 ; echo $? > %s" % (setting['hadoop_dir'], setting['hadoop_dir'], setting['map_size'], setting['dataset'], setting['job_output'], setting['job_log'], setting['job_returncode']) )

			print cmd.command
			os.system(" (%s) &" % cmd.command)

		while True:
			all_pass = True
			for task in returncode_list:
				#print "checking...", task
				if not os.path.exists(task):
					all_pass = False
			if all_pass:
				break
			sleep(1)

		job_id_list = []
		for job_log in log_list:
			job_ids = mylog.lookup_job_ids(job_log)
			job_id_list.extend(job_ids)
		
		# need to modify
		(start_time, end_time) = myinfo.get_elapsed_time(cluster.mapreduce.getResourceManager().host, "19888", job_id_list)
		elapsed_time = end_time- start_time

		elapsed_time_total = 0
		for job_id in job_id_list:
			job_elapsed_time = myinfo.get_job_elapsed_time(cluster.mapreduce.getResourceManager().host, "19888", job_id)
			print "elapsed time: %s sec" % (job_elapsed_time/1000.0)
			print "average storage flow capability: %s MB/s" % (1024.0/(job_elapsed_time/1000.0))
			elapsed_time_total = elapsed_time_total + job_elapsed_time
		print "elapsed time: %s sec" % (elapsed_time/1000.0)
		print "aggregate storage flow capability: %s MB/s" % (job_size * 1024.0 / (elapsed_time/1000.0))



def main(argv):
	parser = argparse.ArgumentParser(description='Configuration generator')
	parser.add_argument("-s", "--size", type=int, required=True, help="The size varies from 1 to the specified GB")

	args = parser.parse_args()
	measure(args.size)

if __name__ == "__main__":
        main(sys.argv[1:])
