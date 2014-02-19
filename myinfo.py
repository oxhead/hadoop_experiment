#!/usr/bin/python

import sys, argparse, requests, json
import mylog

# only at task level, but not task attempt level
def get_task_counters(historyserver_host, historyserver_port, job_id, task_id):
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

def get_task_list(historyserver_host, historyserver_port, job_id):
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

def get_task_detail(historyserver_host, historyserver_port, job_id, task_id):
	task_detail = get_task_counters(historyserver_host, historyserver_port, job_id, task_id)
	return task_detail

def get_task_details(historyserver_host, historyserver_port, job_id):
	task_id = job_id.replace("job_", "task_")
	map_task_id = task_id + "_m_00000"
	reduce_task_id = task_id + "_r_00000"
	map_task_details = get_task_counters(historyserver_host, historyserver_port, job_id, map_task_id)
	reduce_task_details = get_task_counters(historyserver_host, historyserver_port, job_id, reduce_task_id)
	return (map_task_details, reduce_task_details)

def get_job_detail(historyserver_host, historyserver_port, job_id):
	job_url = "http://%s:%s/ws/v1/history/mapreduce/jobs/%s" % (historyserver_host, historyserver_port, job_id);
        job_json_response = requests.get(job_url)
        job_json = job_json_response.json()
        if job_json["job"]["state"] != "SUCCEEDED":
                return;

	counters = {}
        counters['startTime'] = job_json["job"]["startTime"]
        counters['finishTime'] = job_json["job"]["finishTime"]
	return counters

def get_job_elapsed_time(historyserver_host, historyserver_port, job_id):
	job_detail = get_job_detail(historyserver_host, historyserver_port, job_id)
	return job_detail['finishTime'] - job_detail['startTime']	

def get_elapsed_time(historyserver_host, historyserver_port, job_id_list):
	start_time = sys.maxint
	finish_time = -1
	for job_id in job_id_list:
		job_detail = get_job_detail(historyserver_host, historyserver_port, job_id)
		start_time = job_detail['startTime'] if job_detail['startTime'] < start_time else start_time
		finish_time = job_detail['finishTime'] if job_detail['finishTime'] > finish_time else finish_time
	return (start_time, finish_time)

	

def print_flow_detail(historyserver_host, historyserver_port, job_id):
        (map_task_detail, reduce_task_detail) = get_task_details(historyserver_host, historyserver_port, job_id)
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

def get_task_flow_detail(historyserver_host, historyserver_port, job_id, task_id):
	task_detail = get_task_detail(historyserver_host, historyserver_port, job_id, task_id)
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


def main(argv):
        parser = argparse.ArgumentParser(description='Job ID')
        parser.add_argument('-j', '--job_id', metavar='JOB_ID', help='an integer for the accumulator')

        args = parser.parse_args()

        export_job_info(args.job_id)

        sys.exit(0)


if __name__ == "__main__":
        main(sys.argv[1:])
