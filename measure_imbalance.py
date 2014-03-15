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

def run(output_directory, measure_times, num_nodes, model):
	#job_list = ["terasort", "wordcount", "grep", "nocomputation", "classification"]
	job_list = ["terasort", "wordcount", "grep", "nocomputation", "custommap"]
	#job_list = ["grep", "nocomputation", "custommap"]
	# job_size_list = ["64MB", "128MB", "256MB", "512MB", "1GB", "2GB", "4GB", "8GB"]
	job_size_list = ["1GB", "2GB", "4GB", "8GB"]
	map_size = 1024
	prefix="%s-imbalance" % model

	os.system("mkdir -p %s" % output_directory)
	output_file = "%s/measure_imbalance.csv" % output_directory
	output_waiting_time = "%s/waiting_time.csv" % output_directory
	output_ganglia = "%s/ganglia" % output_directory
	output_dstat = "%s/dstat" % output_directory


	fd = open(output_file, "w+")
	myinfo.print_header_to_file(fd)

	for i in range(num_nodes, num_nodes+1):

		configuration = "setting/node_list.py.%s.%sc1s" % (model, i)
		myhadoop.switch_configuration(configuration)
		# wait HDFS to turn off safe mode
		sleep(60)
		myhadoop.prepare_data(job_size_list)
		
		
		job_num= num_nodes * 3 * 2	
		#job_num = num_nodes
		#job_num = 4
		prefix_run = "%s-%sc-%s" % (prefix, job_num, datetime.datetime.now().strftime("%Y%m%d%H%M%S"))
		setting_list = []
		time_start = int(time.time())
                mymonitor.collect_start()
		for j in range(1, job_num+1):
			job_size = job_size_list[random.randint(1, len(job_size_list)-1)]
			real_size = myjob.convert_unit(job_size)
			job = job_list[random.randint(0, len(job_list)-1)]
			num_reducers = random.randint(2, 16)
			job_params = None
			if job == "custommap":
				job_params = {'timeout': '1', 'num_cpu_workers': '1', 'num_vm_workers':'1', 'vm_bytes':str(1024*1024*1)}
				
			setting = myjob.get_job_setting(job, job_params=job_params, map_size=map_size, job_size=real_size, num_reducers=num_reducers, prefix="%s-n%s" % (prefix_run, j))
			myjob.submit_async(setting)
			setting_list.append(setting)
			sleep(random.randint(1, 30))

		myjob.wait_completion(setting_list)

		time_end = int(time.time())
		mymonitor.collect_stop(output_dstat)
		myganglia.collect(output_ganglia, time_start, time_end)
		myreport.report_waiting_time(time_start, time_end, output_waiting_time)

		# make time or historyserver to be ready
		sleep(60)

		myinfo.create_report(setting_list, fd, measure_times)

		for setting in setting_list:
			myjob.clean_job(setting)

		print "Start time:", datetime.datetime.fromtimestamp(time_start).strftime('%Y-%m-%d %H:%M:%S')
		print "End time:", datetime.datetime.fromtimestamp(time_end).strftime('%Y-%m-%d %H:%M:%S')

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
