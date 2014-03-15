#!/usr/bin/python

import sys
import os
import argparse
from time import sleep
import time
import datetime
import numpy

import myjob
import mylog
import myinfo
import myganglia
import mymonitor
import myhadoop
import myreport

def run(output_directory, measure_times, num_nodes, model):
	job_list = ["terasort", "wordcount", "grep", "nocomputation"]
	#job_list = ["grep"]
	job_size = "3GB"
	map_size = 1024
	prefix="weak-scaling-%s" % model

	os.system("mkdir -p %s" % output_directory)
	output_file = "%s/measure_weak_scaling.csv" % output_directory

	fd = open(output_file, "w+")
	myinfo.print_header_to_file(fd)

	for i in range(measure_times):
		for job_num in reversed(range(1, num_nodes+1)):
			configuration = "setting/node_list.py.%s.%sc1s" % (model, job_num)
			myhadoop.switch_configuration(configuration)
                	# wait HDFS to turn off safe mode
                	sleep(60)
                	myhadoop.prepare_data([job_size])

			real_size = myjob.convert_unit(job_size)
			
			prefix_run = "%s-%sc" % (prefix, job_num)
			for job in job_list:
				job_run_dir = "%s/%s_%s" % (output_directory, job_num, job)
				os.system("mkdir -p %s" % job_run_dir)
        			output_waiting_time = "%s/waiting_time.csv" % job_run_dir
        			output_ganglia = "%s/ganglia" % job_run_dir
        			output_dstat = "%s/dstat" % job_run_dir

				time_start = int(time.time())
				mymonitor.collect_start()

				setting_list = []
				for j in range(1, job_num+1):
					setting = myjob.get_job_setting(job, map_size=map_size, job_size=real_size, num_reducers=1, prefix="%s-n%s" % (prefix_run, j))
					myjob.submit_async(setting)
					setting_list.append(setting)

				myjob.wait_completion(setting_list)

				time_end = int(time.time())
				mymonitor.collect_stop(output_dstat)
                		myganglia.collect(output_ganglia, time_start, time_end)
                		myreport.report_waiting_time_by_jobs(setting_list, output_waiting_time)

				# make time or historyserver to be ready
				sleep(60)
				myinfo.create_report(setting_list, fd, measure_times)

				for setting in setting_list:
					myjob.clean_job(setting)
				
				print "Start time:", datetime.datetime.fromtimestamp(time_start).strftime('%Y-%m-%d %H:%M:%S')
 		 	        print "End time:", datetime.datetime.fromtimestamp(time_end).strftime('%Y-%m-%d %H:%M:%S')
				print "Elapsed Time:", time_end-time_start, "seconds"

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
