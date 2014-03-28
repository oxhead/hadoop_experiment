#!/usr/bin/python

import sys
import os
import argparse
from time import sleep
import datetime
import time
import random
import numpy

import myjob
import mylog
import myinfo
import myganglia
import mymonitor
import myhadoop
import myreport
import myexperiment

def create_jobs(num_jobs = 12, job_list=[], job_size_list=[], period=10):
	job_submit_timeline = {}
	# expected time period is 30 seconds
	rate = 1/float(period)
	time_count = 0
	for i in range(1, num_jobs+1):
		job = job_list[random.randint(0, len(job_list)-1)]
		job_size = job_size_list[random.randint(0, len(job_size_list)-1)]
		real_size = myjob.convert_unit(job_size)
		num_reducers = 1 if real_size < 1024 else real_size/1024
		
		job_parameter = {
			'job': job,
			'job_size': job_size,
			'num_reducers': num_reducers,
		}
		
		if time_count not in job_submit_timeline:
			job_submit_timeline[time_count] = []
		job_submit_timeline[time_count].append(job_parameter)
				
		nextTime = int(random.expovariate(rate))
		time_count += nextTime

	return job_submit_timeline

def create_duplicate_jobs(job_list=[], job_size_list=[], submit_times=10):
        job_submit_timeline = {}
        time_count = 0
        for job in job_list:
                for i in range(submit_times):
                        job_size = job_size_list[random.randint(0, len(job_size_list)-1)]
			real_size = myjob.convert_unit(job_size)
                        num_reducers = 1 if real_size < 1024 else real_size/1024
                        job_parameter = {
                                'job': job,
                                'job_size': job_size,
                                'num_reducers': num_reducers,
                        }
                        if time_count not in job_submit_timeline:
                                job_submit_timeline[time_count] = []
                        job_submit_timeline[time_count].append(job_parameter)
        return job_submit_timeline

def create_fixed_jobs(job_list=[], job_size_list=[], submit_times=10):
	job_submit_timeline = {}
	time_count = 0
	for job in job_list:
		for i in range(submit_times):
			job_size = job_size_list[random.randint(0, len(job_size_list)-1)]
			real_size = myjob.convert_unit(job_size)
                        num_reducers = 1 if real_size < 1024 else real_size/1024
			job_parameter = {
				'job': job,
				'job_size': job_size,
				'num_reducers': num_reducers,
			}
			if time_count not in job_submit_timeline:
				job_submit_timeline[time_count] = []
			job_submit_timeline[time_count].append(job_parameter)
		time_count += submit_times*30
	return job_submit_timeline
			
def create_interleave_jobs(job_list=[], job_size_list=[], submit_times=10):
        job_submit_timeline = {}
        time_count = 0
	for i in xrange(submit_times):
       		for job in job_list:
                        job_size = job_size_list[random.randint(0, len(job_size_list)-1)]
			real_size = myjob.convert_unit(job_size)
	                num_reducers = 1 if real_size < 1024 else real_size/1024
                        job_parameter = {
                                'job': job,
                                'job_size': job_size,
                                'num_reducers': num_reducers,
                        }
                        if time_count not in job_submit_timeline:
                                job_submit_timeline[time_count] = []
                        job_submit_timeline[time_count].append(job_parameter)
                time_count += 30
        return job_submit_timeline	
	


def main(argv):
	parser = argparse.ArgumentParser(description='Configuration generator')
        parser.add_argument("-d", "--directory", required=True, help="The output directory")
	parser.add_argument("-m", "--model", default="decoupled", choices=["decoupled", "reference"])
	parser.add_argument("-s", "--scheduler", default="Color", choices=["Flow", "Color", "Fifo"])
	args = parser.parse_args()
	run(args.directory, args.model, args.scheduler)

if __name__ == "__main__":
        main(sys.argv[1:])
