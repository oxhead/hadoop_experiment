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
from command import Command
import myjob
import myinfo
import mycluster
import mylog

def measure(job, map_size, job_size, prefix=None, log_dir="log"):
	real_size = myjob.convert_unit(job_size)
	prefix = prefix if prefix is not None else "job-flow"
	setting = myjob.get_job_setting(job, map_size=map_size, job_size=real_size, num_reducers=1, prefix=prefix, log_dir=log_dir)
	myjob.submit(setting)
	job_ids = mylog.lookup_job_ids(setting['job_log'])
	for job_id in job_ids:
		cluster = mycluster.load()
		#myinfo.print_flow_detail(cluster.mapreduce.getResourceManager().host, "19888", job_id)
		(map_task_list, reduce_task_list) = myinfo.get_task_list(job_id)
		for map_task_id in map_task_list:
			task_detail = myinfo.get_task_flow_detail(job_id, map_task_id)
			myinfo.print_task_flow_detail(task_detail)
		for reduce_task_id in reduce_task_list:
			task_detail = myinfo.get_task_flow_detail(job_id, map_task_id)
                        myinfo.print_task_flow_detail(task_detail)

def main(argv):
	parser = argparse.ArgumentParser(description='Configuration generator')
	parser.add_argument("-m", "--map", type=int, required=False, default=1024, help="The memory usage of each map with the unit MB")
	parser.add_argument("-s", "--size", default="1GB", required=False, help="The data size")
	parser.add_argument("-j", "--job", required=True, action="append", help="The job name to measure flow demand")
	parser.add_argument("--prefix", required=False, help="The prefix of job output")

	args = parser.parse_args()

	for job in args.job:
		measure(job, map_size=args.map, job_size=args.size, prefix=args.prefix)
			

if __name__ == "__main__":
        main(sys.argv[1:])
