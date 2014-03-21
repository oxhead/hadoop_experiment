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
	#job_list = ["terasort", "wordcount", "grep", "nocomputation", "custommap"]
	job_list = ["terasort", "wordcount", "grep", "nocomputation"]
	job_size_list = ["64MB", "128MB", "256MB", "512MB", "1GB", "2GB", "4GB"]
	#job_size_list = ["256MB"]
	map_size = 512
	prefix="scale-computing-single"
	num_computing_nodes = 1
	num_storage_nodes = 1

	iteration = 1

	configuration = "setting/node_list.py.%s.%sc%ss" % (model, num_computing_nodes, num_storage_nodes)
        myhadoop.switch_configuration(configuration)
        sleep(60)
        myhadoop.prepare_data(job_size_list)

	experiment = myexperiment.Experiment(output_directory)
	experiment.start()
	for job in job_list:
		for job_size in job_size_list:
			for i in range(1, iteration+1):
				prefix_run = "%s-i%s" % (prefix, i)
				real_size = myjob.convert_unit(job_size)
				num_reducers = 1
				job_params = None
				if job == "custommap":
					timeout = 1
					job_params = {'timeout': timeout, 'num_cpu_workers': timeout, 'num_vm_workers':'1', 'vm_bytes':str(1024*1024*timeout)}
					
				setting = myjob.get_job_setting(job, job_params=job_params, map_size=map_size, job_size=real_size, num_reducers=num_reducers, prefix=prefix_run)
				experiment.addJob(setting)
				myjob.submit(setting)

	experiment.stop()
	experiment.clean()

def main(argv):
	parser = argparse.ArgumentParser(description='Configuration generator')
        parser.add_argument("-d", "--directory", required=True, help="The output directory")
	parser.add_argument("-m", "--model", default="decoupled", choices=["decoupled", "reference"])
	args = parser.parse_args()
	run(args.directory, args.model)

if __name__ == "__main__":
        main(sys.argv[1:])
