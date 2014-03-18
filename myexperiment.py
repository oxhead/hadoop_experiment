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

class Experiment():
	def __init__(self, output_directory):
		self.output_directory = output_directory
		self.time_start = None
		self.time_end = None

		os.system("mkdir -p %s" % output_directory)
		self.output_file = "%s/measure_imbalance.csv" % output_directory
		self.output_waiting_time = "%s/waiting_time.csv" % output_directory
		self.output_ganglia = "%s/ganglia" % output_directory
		self.output_dstat = "%s/dstat" % output_directory
		self.output_flow_timeline = "%s/flow_timeline.csv" % output_directory
		self.output_task_timeline = "%s/task_timeline.csv" % output_directory

	def start(self):
		self.time_start = int(time.time())
		mymonitor.collect_start()

	def stop(self):
		self.time_end = int(time.time())
		mymonitor.collect_stop(self.output_dstat)
		myganglia.collect(self.output_ganglia, self.time_start, self.time_end)
		print "Status: Wait for completion of HDFS service (60 sec)"

		sleep(60)
		print "[Report] Waiting time"
		myreport.report_waiting_time(self.time_start, self.time_end, self.output_waiting_time)
		print "[Report] Task timeline"
		myreport.report_task_timeline(self.time_start, self.time_end, self.output_task_timeline)
		print "[Report] Flow timeline"
		myreport.report_flow_timeline(self.time_start, self.time_end, self.output_flow_timeline)

		print "[Time] Start  :", datetime.datetime.fromtimestamp(self.time_start).strftime('%Y-%m-%d %H:%M:%S')
		print "[Time] End    :", datetime.datetime.fromtimestamp(self.time_end).strftime('%Y-%m-%d %H:%M:%S')
		print "[Time] Elapsed:", self.time_end - self.time_start, "sec"

	def clean(self, setting_list):
		for setting in setting_list:
                	myjob.clean_job(setting)

def main(argv):
	parser = argparse.ArgumentParser(description='Configuration generator')
        parser.add_argument("-d", "--directory", required=True, help="The output directory")
	parser.add_argument("-i", "--iteration", type=int, default=3, required=False, help="The number of experiments to run")
	parser.add_argument("-n", "--node", type=int, default=1, required=False, help="The number of nodes")
	parser.add_argument("-m", "--model", default="decoupled", choices=["decoupled", "reference"])
	args = parser.parse_args()
	run(args.directory, args.iteration, args.node, args.model)

if __name__ == "__main__":
        main(sys.argv[1:])
