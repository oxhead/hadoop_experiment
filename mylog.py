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
import myinfo

def lookup_job_ids(job_log):
	job_ids = Command("grep 'mapreduce.Job: Running job:' %s | tr -s ' ' | cut -d' ' -f7" % job_log).run().output.strip()
	return job_ids.split("\n")

def main(argv):
	'''
	parser = argparse.ArgumentParser(description='Configuration generator')
	parser.add_argument("-m", "--map", type=int, required=False, default=1024, help="The memory usage of each map with the unit MB")
	parser.add_argument("-r", "--reducer", type=int, required=False, default=1, help="The number of reduce slots")
	parser.add_argument("-s", "--size", type=int, required=True, choices=[64, 1024], help="The data size")
	parser.add_argument("-j", "--job", required=True, help="The job name to measure flow demand")

	args = parser.parse_args()
	job_ids = submit(args.job, map_size=args.map, task_size=args.size, num_reducers=args.reducer)
	for job_id in job_ids:
		myjob.print_detail(job_id)
	'''

if __name__ == "__main__":
        main(sys.argv[1:])
