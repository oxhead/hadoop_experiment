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

def run(output_directory, scheduler):
	#job_list = ["terasort", "wordcount", "grep", "nocomputation", "custommap"]
	job_set = {
		"wordcount": 3,
		"grep": 9,
	}
	job_size = "16GB"
	map_size = 1024
	prefix="flow-contention"
	num_nodes = 4
	model = "decoupled"

	configuration = "setting/node_list.py.%s.%sc%ss" % (model, num_nodes, 1)
        myhadoop.switch_configuration(configuration, scheduler)
        sleep(60)
        myhadoop.prepare_data([job_size])

	experiment = myexperiment.Experiment(output_directory)
	experiment.start()

	setting_set = {}
	for (job, num) in job_set.iteritems():
		real_size = myjob.convert_unit(job_size)
                num_reducers = 8
		job_params = None
		setting_set[job] = []
		for i in range(1, num+1):
			prefix_run = "%s-j%s" % (prefix, i)
			setting = myjob.get_job_setting(job, job_params=job_params, map_size=map_size, job_size=real_size, num_reducers=num_reducers, prefix=prefix_run)
			setting_set[job].append(setting)
			experiment.addJob(setting)
	myjob.submit_threads(setting_set)

	myreport.report_task_detail_timeline_by_jobs(experiment.setting_list, "%s/task_detail_timeline.csv" % output_directory)
	experiment.stop()
	experiment.clean(experiment.setting_list)

def main(argv):
	parser = argparse.ArgumentParser(description='Configuration generator')
        parser.add_argument("-d", "--directory", required=True, help="The output directory")
	parser.add_argument("-s", "--scheduler", default="fifo", choices=["fifo", "flow", "balancing"])
	args = parser.parse_args()
	run(args.directory, args.scheduler)

if __name__ == "__main__":
        main(sys.argv[1:])
