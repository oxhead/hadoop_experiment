#!/usr/bin/python

import sys
import os
import argparse
from time import sleep
import time
import numpy

import myjob
import mylog
import myinfo
import myganglia
import mymonitor

def run(output_file, measure_times, num_nodes, model):
	job_list = ["terasort", "wordcount", "grep", "nocomputation"]
	job_size = "3GB"
	map_size = 1024
	prefix="weak-scaling-%s" % model

	fd = open(output_file, "w+")
	print >> fd, \
		"job", "order", "map_size", "job_size", \
		"job_elpased_time", \
		"map_elapsed_time_mean", "map_elapsed_time_std", \
		"map_flow_in_mean", "map_flow_in_std", \
		"map_flow_out_mean", "map_flow_out_std", \
		"reduce_elapsed_time_mean", "reduce_elapsed_time_std", \
		"reduce_flow_in_mean", "reduce_flow_in_std", \
		"reduce_flow_out_mean", "reduce_flow_out_std"

	for i in range(measure_times):
		for job_num in range(1, num_nodes+1):
			configuration = "setting/node_list.py.%s.%sc1s" % (model, job_num)
			print "Switch to configuration: %s" % configuration
			os.system("%s service.py --user chsu6 stop all" % sys.executable)
			os.system("cp %s node_list.py" % configuration)
			os.system("%s service.py --user chsu6 format hdfs" % sys.executable)
			os.system("%s deploy.py --user chsu6" % sys.executable)
			os.system("%s service.py --user chsu6 start all" % sys.executable)
			real_size = myjob.convert_unit(job_size)
			# wait HDFS to turn off safe mode
			sleep(60)
			os.system("%s prepare_data.py -u chsu6 -t terasort" % sys.executable)
			os.system("%s prepare_data.py -u chsu6 -t wikipedia -d /nfs_power2/dataset" % sys.executable)
			
			prefix_run = "%s-%sc" % (prefix, job_num)
			for job in job_list:
				time_start = int(time.time())
				mymonitor.collect_start()

				setting_list = []
				for j in range(1, job_num+1):
					setting = myjob.get_job_setting(job, map_size=map_size, job_size=real_size, num_reducers=1, prefix="%s-n%s" % (prefix_run, j))
					myjob.submit_async(setting)
					setting_list.append(setting)

				myjob.wait_completion(setting_list)

				time_end = int(time.time())
				mymonitor.collect_stop("download/%s_%s_%s" % (prefix_run, job, str(i)))
				myganglia.collect("download/%s_%s_%s" % (prefix_run, job, str(i)), time_start, time_end)

				# make time or historyserver to be ready
				sleep(5)

				for setting in setting_list:
					job_log = setting['job_log']
        				job_ids = mylog.lookup_job_ids(job_log)
					job_id = job_ids[0]

                			(map_task_list, reduce_task_list) = myinfo.get_task_list(job_id)
					print_statistics_to_file(job, job_size, map_size, job_id, map_task_list, reduce_task_list, fd, i)

				for setting in setting_list:
					myjob.clean_job(setting)

def print_statistics_to_file(job, job_size, map_size, job_id, map_task_list, reduce_task_list, fd, iteration):
	elapsed_time = myinfo.get_job_elapsed_time(job_id)
	map_flow_in = []
	map_flow_out = []
	map_elapsed_time = []
	reduce_flow_in = []
	reduce_flow_out = []
	reduce_elapsed_time = []
        for map_task_id in map_task_list:
        	task_detail = myinfo.get_task_flow_detail(job_id, map_task_id)
		map_flow_in.append(task_detail['flow_in'])
		map_flow_out.append(task_detail['flow_out'])
		map_elapsed_time.append(task_detail['elapsed_time'])
				
        for reduce_task_id in reduce_task_list:
               	task_detail = myinfo.get_task_flow_detail(job_id, reduce_task_id)
		reduce_flow_in.append(task_detail['flow_in'])
                reduce_flow_out.append(task_detail['flow_out'])
                reduce_elapsed_time.append(task_detail['elapsed_time'])	

	print >> fd, \
		job, iteration, map_size, job_size, \
		elapsed_time, \
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
	parser.add_argument("-m", "--model", default="decoupled", choices=["decoupled", "reference"])
	args = parser.parse_args()
	run(args.output, args.iteration, args.node, args.model)

if __name__ == "__main__":
        main(sys.argv[1:])
