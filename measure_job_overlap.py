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
from retrieve_task_info import *
import myjob
import myinfo
import mycluster
import mylog

def convert_unit(size):
	if "MB" in size:
		return int(size.replace("MB", ""))
	elif "GB" in size:
		return int(size.replace("GB", "")) * 1024

def measure(jobs, map_size, job_size):
	real_size = convert_unit(job_size)

	log_list = []
	returncode_list = []
	for job in jobs:
		setting = myjob.submit_async(job, map_size=map_size, job_size=real_size, num_reducers=1, prefix="job-flow")
		log_list.append(setting['job_log'])
                returncode_list.append(setting['job_returncode'])

	myjob.wait_completion(returncode_list)

	job_id_list = []
        for job_log in log_list:
        	job_ids = mylog.lookup_job_ids(job_log)
                job_id_list.extend(job_ids)
	
	for job_id in job_id_list:
		print "#", job_id
		cluster = mycluster.load()
		#myinfo.print_flow_detail(cluster.mapreduce.getResourceManager().host, "19888", job_id)
		(map_task_list, reduce_task_list) = myinfo.get_task_list(cluster.mapreduce.getResourceManager().host, "19888", job_id)
		for map_task_id in map_task_list:
			myinfo.print_task_flow_detail(cluster.mapreduce.getResourceManager().host, "19888", job_id, map_task_id)
		for reduce_task_id in reduce_task_list:
                        myinfo.print_task_flow_detail(cluster.mapreduce.getResourceManager().host, "19888", job_id, reduce_task_id)

def main(argv):
	parser = argparse.ArgumentParser(description='Configuration generator')
	parser.add_argument("-m", "--map", type=int, required=False, default=1024, help="The memory usage of each map with the unit MB")
	parser.add_argument("-s", "--size", required=False, help="The data size")
	parser.add_argument("-j", "--job", required=True, action="append", help="The job name to measure flow demand")

	args = parser.parse_args()

	measure(args.job, map_size=args.map, job_size=args.size)
			

if __name__ == "__main__":
        main(sys.argv[1:])
