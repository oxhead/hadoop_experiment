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

def run(output_directory, model, scheduler, concurrent=False):
	#job_list = ["grep", "terasort", "wordcount", "nocomputation"]
	job_list = ["invertedindex", "histogrammovies", "histogramratings"]
	#job_list = ["termvector", "histogramratings"]
	job_size_list = ["1GB"]
	map_size = 1024
	prefix="test-hadoop"
	num_computing_nodes = 1
	num_storage_nodes = 1

	configuration = "setting/node_list.py.%s.%sc%ss" % (model, num_computing_nodes, num_storage_nodes)
        myhadoop.switch_configuration(configuration, scheduler)
        sleep(60)
        myhadoop.prepare_data(job_size_list)

	experiment = myexperiment.Experiment(output_directory)
	experiment.start()
	for job in job_list:
		for job_size in job_size_list:
			prefix_run = prefix
			real_size = myjob.convert_unit(job_size)
			num_reducers = 2
			job_params = None
			setting = myjob.get_job_setting(job, job_params=job_params, map_size=map_size, job_size=real_size, num_reducers=num_reducers, prefix=prefix_run)
			experiment.addJob(setting)
			if concurrent:
				myjob.submit_async(setting)
			else:
				myjob.submit(setting)

	if concurrent:
		myjob.wait_completion(experiment.setting_list)

	experiment.stop()
	experiment.clean()

def main(argv):
	parser = argparse.ArgumentParser(description='Configuration generator')
        parser.add_argument("-d", "--directory", required=True, help="The output directory")
	parser.add_argument("-m", "--model", default="decoupled", choices=["decoupled", "reference"])
	parser.add_argument("-s", "--scheduler", default="Fifo", choices=["Fifo", "Color", "ColorStorage"])
	parser.add_argument("-c", "--concurrent", action='store_true')
	args = parser.parse_args()
	run(args.directory, args.model, args.scheduler, args.concurrent)

if __name__ == "__main__":
        main(sys.argv[1:])
