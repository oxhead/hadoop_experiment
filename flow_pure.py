#!/usr/bin/python

import sys
import os
import argparse
from time import sleep
import numpy

import myjob
import mylog
import myinfo
import mycluster

def run(output_file, measure_times, num_nodes):
	job_list = ["terasort", "wordcount", "grep", "nocomputation"]
	job_size_list = ["64MB", "128MB", "256MB", "512MB", "1GB", "2GB", "4GB"]
	map_size_list = [1024]
	prefix="job-flow"

	fd = open(output_file, "w+")
	print >> fd, \
		"job", "order", "map_size", "job_size", \
		"map_elapsed_time_mean", "map_elapsed_time_std", \
		"map_flow_in_mean", "map_flow_in_std", \
		"map_flow_out_mean", "map_flow_out_std", \
		"reduce_elapsed_time_mean", "reduce_elapsed_time_std", \
		"reduce_flow_in_mean", "reduce_flow_in_std", \
		"reduce_flow_out_mean", "reduce_flow_out_std"
	param_list = [(job, job_size, map_size) for job in job_list for job_size in job_size_list for map_size in map_size_list]
	for i in range(measure_times):
		for param in param_list:
			(job, job_size, map_size) = param
			real_size = myjob.convert_unit(job_size)
			
			log_list = []
			returncode_list = []
			setting_list = []
			for j in range(num_nodes):
        			setting = myjob.submit_async(job, map_size=map_size, job_size=real_size, num_reducers=1, prefix="%s-n%s" % (prefix, j))
				log_list.append(setting['job_log'])
				returncode_list.append(setting['job_returncode'])

			myjob.wait_completion(returncode_list)

			# make time or historyserver to be ready
			sleep(5)

			cluster = mycluster.load()
			for job_log in log_list:
        			job_ids = mylog.lookup_job_ids(job_log)
				job_id = job_ids[0]

                		(map_task_list, reduce_task_list) = myinfo.get_task_list(cluster.mapreduce.getResourceManager().host, "19888", job_id)
				print_statistics_to_file(job, job_size, map_size, job_id, map_task_list, reduce_task_list, fd, i)

			for setting in setting_list:
				myjob.clean_job(setting)

def print_statistics_to_file(job, job_size, map_size, job_id, map_task_list, reduce_task_list, fd, iteration):
	cluster = mycluster.load()
	map_flow_in = []
	map_flow_out = []
	map_elapsed_time = []
	reduce_flow_in = []
	reduce_flow_out = []
	reduce_elapsed_time = []
        for map_task_id in map_task_list:
        	task_detail = myinfo.get_task_flow_detail(cluster.mapreduce.getResourceManager().host, "19888", job_id, map_task_id)
		map_flow_in.append(task_detail['flow_in'])
		map_flow_out.append(task_detail['flow_out'])
		map_elapsed_time.append(task_detail['elapsed_time'])
				
        for reduce_task_id in reduce_task_list:
               	task_detail = myinfo.get_task_flow_detail(cluster.mapreduce.getResourceManager().host, "19888", job_id, reduce_task_id)
		reduce_flow_in.append(task_detail['flow_in'])
                reduce_flow_out.append(task_detail['flow_out'])
                reduce_elapsed_time.append(task_detail['elapsed_time'])	

	print >> fd, \
		job, iteration, map_size, job_size, \
		numpy.mean(map_elapsed_time), numpy.std(map_elapsed_time), \
		numpy.mean(map_flow_in), numpy.std(map_flow_in), \
		numpy.mean(map_flow_out), numpy.std(map_flow_out), \
		numpy.mean(reduce_elapsed_time), numpy.std(reduce_elapsed_time), \
		numpy.mean(reduce_flow_in), numpy.std(reduce_flow_in), \
		numpy.mean(reduce_flow_out), numpy.std(reduce_flow_out)
	fd.flush()

def main(argv):
	parser = argparse.ArgumentParser(description='Configuration generator')
        parser.add_argument("-o", "--output", required=True, help="The output file")
	parser.add_argument("-i", "--iteration", type=int, default=3, required=False, help="The number of experiments to run")
	parser.add_argument("-n", "--node", type=int, default=1, required=False, help="The number of nodes")
	args = parser.parse_args()
	run(args.output, args.iteration, args.node)

if __name__ == "__main__":
        main(sys.argv[1:])
