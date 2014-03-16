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

def run(output_directory, model):
	job_list = ["terasort", "wordcount", "grep", "nocomputation", "custommap"]
	# job_size_list = ["64MB", "128MB", "256MB", "512MB", "1GB", "2GB", "4GB", "8GB"]
	job_size = "64MB"
	map_size = 1024
	prefix="flow_demand"

	iteration = 1

	configuration = "setting/node_list.py.%s.%sc1s" % (model, 1)
        myhadoop.switch_configuration(configuration)
        # wait HDFS to turn off safe mode
        sleep(60)
        myhadoop.prepare_data([job_size])

	experiment = myexperiment.Experiment(output_directory)
	experiment.start()
	setting_list = []
	for job in job_list:
		for i in range(1, iteration+1):
			prefix_run = "%s-%s-%s" % (prefix, job, i)
			real_size = myjob.convert_unit(job_size)
			num_reducers = 1
			job_params = None
			if job == "custommap":
				job_params = {'timeout': '1', 'num_cpu_workers': '1', 'num_vm_workers':'1', 'vm_bytes':str(1024*1024*1)}
				
			setting = myjob.get_job_setting(job, job_params=job_params, map_size=map_size, job_size=real_size, num_reducers=num_reducers, prefix="%s-n%s" % (prefix_run, i))
			myjob.submit_async(setting)
			setting_list.append(setting)
			myjob.wait_completion([setting])

	myreport.report_flow_demand_by_jobs(setting_list, "%s/flow_demand.csv" % output_directory)
	experiment.stop()

	for setting in setting_list:
		myjob.clean_job(setting)

def main(argv):
	parser = argparse.ArgumentParser(description='Configuration generator')
        parser.add_argument("-d", "--directory", required=True, help="The output directory")
	parser.add_argument("-m", "--model", default="decoupled", choices=["decoupled", "reference"])
	args = parser.parse_args()
	run(args.directory, args.model)

if __name__ == "__main__":
        main(sys.argv[1:])
