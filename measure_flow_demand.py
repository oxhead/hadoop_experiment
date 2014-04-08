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
	#job_list = ["terasort", "wordcount", "grep", "nocomputation", "custommap3"]
	#job_list = ["custommap1", "custommap2", "custommap3", "custommap4", "custommap5", "custommap6"]
	job_list = ["terasort", "wordcount", "grep", "nocomputation", "custommap1", "custommap2", "custommap3", "custommap4", "custommap5", "custommap6", "histogrammovies", "histogramratings", "invertedindex"]
	#job_list = ["invertedindex", "grep", "wordcount", "terasort"]

	# job_size_list = ["64MB", "128MB", "256MB", "512MB", "1GB", "2GB", "4GB", "8GB"]
	job_size = "64MB"
	map_size = 1024
	prefix="flow-demand"

	iteration = 1

	configuration = "setting/node_list.py.%s.%sc1s" % (model, 1)
        myhadoop.switch_configuration(configuration)
        # wait HDFS to turn off safe mode
        sleep(60)
        myhadoop.prepare_data([job_size])

	experiment = myexperiment.Experiment(output_directory)
	experiment.start()
	for job in job_list:
		for i in range(1, iteration+1):
			prefix_run = "%s-i%s" % (prefix, i)
			real_size = myjob.convert_unit(job_size)
			num_reducers = 1
				
			setting = myjob.get_job_setting(job, map_size=map_size, job_size=real_size, num_reducers=num_reducers, prefix=prefix_run)
			myjob.submit_async(setting)
			myjob.wait_completion([setting])
			experiment.addJob(setting)

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
