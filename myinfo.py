#!/usr/bin/python

import sys, argparse, requests, json
import mylog
import mycluster
from node_list import historyserver
import numpy

historyserver_host = historyserver['host']
historyserver_port = historyserver['port']

# only at task level, but not task attempt level
def get_task_counters(job_id, task_id):
	task_url = "http://%s:%s/ws/v1/history/mapreduce/jobs/%s/tasks/%s" % (historyserver_host, historyserver_port, job_id, task_id);
        task_json_response = requests.get(task_url)
        task_json = task_json_response.json()
        if task_json["task"]["state"] != "SUCCEEDED":
                return;

	counters = {}
	counters['type'] = task_json["task"]["type"]
	counters['startTime'] = task_json["task"]["startTime"]
	counters['finishTime'] = task_json["task"]["finishTime"]
	
	counters_url = "%s/counters" % task_url
	counters_json_response = requests.get(counters_url)
        counters_json = counters_json_response.json()

	# initialize
	counters['MAP_OUTPUT_MATERIALIZED_BYTES'] = 0
	for counter_group in counters_json['jobTaskCounters']['taskCounterGroup']:
		for counter in counter_group['counter']:
			if "MAP" == counters["type"]:
				if counter_group['counterGroupName'] == "org.apache.hadoop.mapreduce.FileSystemCounter":
					pass
				elif counter_group['counterGroupName'] == "org.apache.hadoop.mapreduce.TaskCounter":
					if counter['name'] == "MAP_OUTPUT_MATERIALIZED_BYTES":
                                        	counters['MAP_OUTPUT_MATERIALIZED_BYTES'] = counter['value']
				elif counter_group['counterGroupName'] == "org.apache.hadoop.mapreduce.lib.input.FileInputFormatCounter":
					if counter['name'] == "BYTES_READ":
						counters['BYTES_READ'] = counter['value']
			elif "REDUCE" == counters["type"]:
				if counter_group['counterGroupName'] == "org.apache.hadoop.mapreduce.FileSystemCounter":
                                        pass
                                elif counter_group['counterGroupName'] == "org.apache.hadoop.mapreduce.TaskCounter":
                                        if counter['name'] == "REDUCE_SHUFFLE_BYTES":
                                                counters['REDUCE_SHUFFLE_BYTES'] = counter['value']
                                elif counter_group['counterGroupName'] == "org.apache.hadoop.mapreduce.lib.output.FileOutputFormatCounter":
                                        if counter['name'] == "BYTES_WRITTEN":
                                                counters['BYTES_WRITTEN'] = counter['value']

	return counters

def get_task_list(job_id):
	tasks_url = "http://%s:%s/ws/v1/history/mapreduce/jobs/%s/tasks" % (historyserver_host, historyserver_port, job_id);
        tasks_json_response = requests.get(tasks_url)
        tasks_json = tasks_json_response.json()

	map_task_list = []
	reduce_task_list = []
	for task in tasks_json['tasks']['task']:
		if task['state'] == "SUCCEEDED":
			if task['type'] == "MAP":
				map_task_list.append(task['id'])
			elif task['type'] == "REDUCE":
				reduce_task_list.append(task['id'])
	return (map_task_list, reduce_task_list)

def get_task_detail(job_id, task_id):
	task_detail = get_task_counters(job_id, task_id)
	return task_detail

def get_task_details(job_id):
	task_id = job_id.replace("job_", "task_")
	map_task_id = task_id + "_m_00000"
	reduce_task_id = task_id + "_r_00000"
	map_task_details = get_task_counters(job_id, map_task_id)
	reduce_task_details = get_task_counters(job_id, reduce_task_id)
	return (map_task_details, reduce_task_details)

def get_job_detail(job_id):
	job_url = "http://%s:%s/ws/v1/history/mapreduce/jobs/%s" % (historyserver_host, historyserver_port, job_id);
        job_json_response = requests.get(job_url)
        job_json = job_json_response.json()
        if job_json["job"]["state"] != "SUCCEEDED":
                return;

	counters = {}
        counters['startTime'] = job_json["job"]["startTime"]
        counters['finishTime'] = job_json["job"]["finishTime"]
	return counters

def get_job_elapsed_time(job_id):
	job_detail = get_job_detail(job_id)
	return job_detail['finishTime'] - job_detail['startTime']	

def get_elapsed_time(job_id_list):
	start_time = sys.maxint
	finish_time = -1
	for job_id in job_id_list:
		job_detail = get_job_detail(job_id)
		start_time = job_detail['startTime'] if job_detail['startTime'] < start_time else start_time
		finish_time = job_detail['finishTime'] if job_detail['finishTime'] > finish_time else finish_time
	return (start_time, finish_time)

	

def print_flow_detail(job_id):
        (map_task_detail, reduce_task_detail) = get_task_details(job_id)
        elapsed_map_time = int(map_task_detail['finishTime']) - int(map_task_detail['startTime'])
        elapsed_reduce_time = int(reduce_task_detail['finishTime']) - int(reduce_task_detail['startTime'])
        map_input_size = int(map_task_detail['BYTES_READ'])
        map_output_size = int(map_task_detail['MAP_OUTPUT_MATERIALIZED_BYTES'])
        reduce_input_size = int(reduce_task_detail['REDUCE_SHUFFLE_BYTES'])
        reduce_output_size = int(reduce_task_detail['BYTES_WRITTEN'])

        print "elapsed map time: %s sec" % (elapsed_map_time/1000.0)
        print "elapsed reduce time: %s sec" % (elapsed_reduce_time/1000.0)
        print "average map in-flow demand: %s MB/s" % ( map_input_size / 1024.0 / elapsed_map_time)
        print "average map out-flow demand: %s MB/s" % ( map_output_size / 1024.0 / elapsed_reduce_time)
        print "average reduce in-flow demand: %s MB/s" % ( reduce_input_size / 1024.0 / elapsed_reduce_time)
        print "average reduce out-flow demand: %s MB/s" % ( reduce_output_size / 1024.0 / elapsed_reduce_time)

def print_task_flow_detail(task_detail):
	print "elapsed %s time: %s sec" % (task_detail['type'].lower(), task_detail['elapsed_time'])
        print "average %s flow-in demand: %s MB/s" % (task_detail['type'].lower(), task_detail['flow_in'])
        print "average %s flow-out demand: %s MB/s" % (task_detail['type'].lower(), task_detail['flow_out'])

def get_task_flow_detail(job_id, task_id):
	task_detail = get_task_detail(job_id, task_id)
        elapsed_task_time = int(task_detail['finishTime']) - int(task_detail['startTime'])
	task_detail['elapsed_time'] = elapsed_task_time / 1000.0
        if task_detail['type'] == "MAP":
		task_detail['input_size'] = int(task_detail['BYTES_READ'])
		task_detail['output_size'] = int(task_detail['MAP_OUTPUT_MATERIALIZED_BYTES'])
		task_detail['flow_in'] = float( task_detail['input_size'] / (1024*1024) / task_detail['elapsed_time'])
		task_detail['flow_out'] = float( task_detail['output_size'] / (1024*1024) / task_detail['elapsed_time'])
        elif task_detail['type'] == "REDUCE":
		task_detail['input_size'] = int(task_detail['REDUCE_SHUFFLE_BYTES'])
                task_detail['output_size'] = int(task_detail['BYTES_WRITTEN'])
		task_detail['flow_in'] = float( task_detail['input_size'] / (1024*1024) / task_detail['elapsed_time'])
                task_detail['flow_out'] = float( task_detail['output_size'] / (1024*1024) / task_detail['elapsed_time'])
	return task_detail

def create_report(setting_list, fd, measure_times):
	for setting in setting_list:
        	job_log = setting['job_log']
                job_ids = mylog.lookup_job_ids(job_log)
                job_id = job_ids[0]

		list = None
		try:
			print job_id, job_log
			list = get_task_list(job_id)
		except:
			print "error to get task list for job: %s" % job_id
			pass

		if list is not None:
                	(map_task_list, reduce_task_list) = list
                	print_statistics_to_file(job_id, setting, map_task_list, reduce_task_list, fd, measure_times)
		else:
			print "Unable to retrieve job info for %s" % job_id

def print_header_to_file(fd):
	print >> fd, \
                "job", "order", "map_size", "job_size", \
                "job_elpased_time", \
                "map_elapsed_time_mean", "map_elapsed_time_std", \
                "map_flow_in_mean", "map_flow_in_std", \
                "map_flow_out_mean", "map_flow_out_std", \
                "reduce_elapsed_time_mean", "reduce_elapsed_time_std", \
                "reduce_flow_in_mean", "reduce_flow_in_std", \
                "reduce_flow_out_mean", "reduce_flow_out_std"

def print_statistics_to_file(job_id, setting, map_task_list, reduce_task_list, fd, iteration):
	job = setting['job']
	job_size = setting['job_size']
	map_size = setting['map_size']
        elapsed_time = get_job_elapsed_time(job_id)
        map_flow_in = []
        map_flow_out = []
        map_elapsed_time = []
        reduce_flow_in = []
        reduce_flow_out = []
        reduce_elapsed_time = []
        for map_task_id in map_task_list:
                task_detail = get_task_flow_detail(job_id, map_task_id)
                map_flow_in.append(task_detail['flow_in'])
                map_flow_out.append(task_detail['flow_out'])
                map_elapsed_time.append(task_detail['elapsed_time'])

        for reduce_task_id in reduce_task_list:
                task_detail = get_task_flow_detail(job_id, reduce_task_id)
                reduce_flow_in.append(task_detail['flow_in'])
                reduce_flow_out.append(task_detail['flow_out'])
                reduce_elapsed_time.append(task_detail['elapsed_time'])

        print >> fd, \
                job, ",", iteration, ",", map_size, ",", job_size, ",", \
                elapsed_time, ",", \
                numpy.mean(map_elapsed_time), ",", numpy.std(map_elapsed_time), ",",\
                numpy.mean(map_flow_in), ",", numpy.std(map_flow_in), ",", \
                numpy.mean(map_flow_out), ",", numpy.std(map_flow_out), ",", \
                numpy.mean(reduce_elapsed_time), ",", numpy.std(reduce_elapsed_time), ",", \
                numpy.mean(reduce_flow_in), ",", numpy.std(reduce_flow_in), ",", \
                numpy.mean(reduce_flow_out), ",", numpy.std(reduce_flow_out)
        fd.flush()


def main(argv):
        parser = argparse.ArgumentParser(description='Job ID')
        parser.add_argument('-j', '--job_id', metavar='JOB_ID', help='an integer for the accumulator')

        args = parser.parse_args()

        export_job_info(args.job_id)

        sys.exit(0)


if __name__ == "__main__":
        main(sys.argv[1:])
