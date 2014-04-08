#!/usr/bin/python

import sys
import os
import argparse
from time import sleep
import datetime
import time
import random
import numpy
import itertools

import myjob
import mylog
import myinfo
import myganglia
import mymonitor
import myhadoop
import myreport
import myexperiment
import job_factory

def run(output_directory, model):
	scheduler_list = ["ColorStorage", "Fifo"] 
	#scheduler_list = ["Fifo", "Color"]
	#scheduler_list = ["Color"]

	job_list = ["histogrammovies", "histogramratings"]
	#job_list = ["grep", "terasort", "wordcount", "nocomputation", "custommap4", "custommap6"]
	#job_size_list = ["64MB", "128MB", "256MB", "512MB", "1GB", "2GB", "4GB"]
	job_size_list = ["8GB"]
	submit_times = 10

	job_combination_list = itertools.combinations(job_list, 2)

	for (x, y) in job_combination_list:
		print "%s+%s" % (x, y)
		job_list = [x, y]
		job_submit_timeline = job_factory.create_duplicate_jobs(job_list=job_list, job_size_list=job_size_list, submit_times=submit_times)
			
		for scheduler in scheduler_list:
			output_directory_run = "%s/%s_%s_%s" % (output_directory, scheduler, x, y)
			scheduler_run(job_submit_timeline, output_directory_run, scheduler, model, job_size_list)

def scheduler_run(job_timeline, output_directory, scheduler, model, job_size_list):
	map_size = 1024
	prefix="flow-contention"
	num_computing_nodes = 2
	num_storage_nodes = 2
	configuration = "setting/node_list.py.%s.%sc%ss" % (model, num_computing_nodes, num_storage_nodes)
        myhadoop.switch_configuration(configuration, scheduler)
        # wait HDFS to turn off safe mode
        sleep(60)
        myhadoop.prepare_data(job_size_list)

	experiment = myexperiment.Experiment(output_directory)
	experiment.start()
	current_time = 0
	for key in sorted(job_timeline.keys()):
		value = job_timeline[key]
		if (key>current_time):
			time.sleep(key-current_time)
		current_time = key	

		for i in range(len(value)):
			job_parameter = value[i]
			prefix_run = "%s-t%s-j%s" % (prefix, key, i)
			job = job_parameter['job']
			job_size = job_parameter['job_size']
			real_size = myjob.convert_unit(job_size)
			num_reducers = job_parameter['num_reducers']
			job_params = None
				
			setting = myjob.get_job_setting(job, job_params=job_params, map_size=map_size, job_size=real_size, num_reducers=num_reducers, prefix=prefix_run)
			myjob.submit_async(setting)
			experiment.addJob(setting)

	myjob.wait_completion(experiment.setting_list)

	experiment.stop()
	experiment.clean()

def main(argv):
	parser = argparse.ArgumentParser(description='Configuration generator')
        parser.add_argument("-d", "--directory", required=True, help="The output directory")
	parser.add_argument("-m", "--model", default="decoupled", choices=["decoupled", "reference"])
	parser.add_argument("-s", "--scheduler", default="Color", choices=["Flow", "Color", "Fifo"])
	args = parser.parse_args()
	run(args.directory, args.model)

if __name__ == "__main__":
        main(sys.argv[1:])
