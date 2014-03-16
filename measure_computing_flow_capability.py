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
	job = "custommap"
	job_params = {'timeout': '0', 'num_cpu_workers': '1', 'num_vm_workers':'1', 'vm_bytes':str(1024*1024*1)}
	job_size_list = ["64MB", "128MB", "256MB", "512MB", "1GB", "2GB", "4GB", "8GB"]
	#job_size_list = ["8GB"]
	map_size = 512
	prefix="computing-flow-capability"

	configuration = "setting/node_list.py.%s.%sc%ss" % (model, 1, 4)
        myhadoop.switch_configuration(configuration)
        # wait HDFS to turn off safe mode
        sleep(60)
        myhadoop.prepare_data(job_size_list)

	#experiment = myexperiment.Experiment(output_directory)
	#experiment.start()
	setting_list = []
	for job_size in job_size_list:
		
		prefix_run = "%s-%s-%s" % (prefix, job, job_size)
		real_size = myjob.convert_unit(job_size)
		num_reducers = 1
		setting = myjob.get_job_setting(job, job_params=job_params, map_size=map_size, job_size=real_size, num_reducers=num_reducers, prefix=prefix)
		myjob.submit_async(setting)
		setting_list.append(setting)
		myjob.wait_completion([setting])

	myreport.report_computing_flow_capability_by_jobs(setting_list, "%s/%s" % (output_directory, prefix))
	#experiment.stop()

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
