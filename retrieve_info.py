#!/usr/bin/python

import sys, argparse, requests, json

def get_job_details(historyserver_host, historyserver_port, job_id):
	


# only at task level, but not task attempt level
def get_task_counters(historyserver_host, historyserver_port, job_id, task_id):
	task_url = "http://%s:%s/ws/v1/history/mapreduce/jobs/%s/tasks/%s" % (historyserver_host, historyserver_port, job_id, task_id);
        print task_url
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
	print counters_json 

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

def get_task_details(historyserver_host, historyserver_port, job_id):
	task_id = job_id.replace("job_", "task_")
	map_task_id = task_id + "_m_00000"
	reduce_task_id = task_id + "_r_00000"
	map_task_details = get_task_counters(historyserver_host, historyserver_port, job_id, map_task_id)
	reduce_task_details = get_task_counters(historyserver_host, historyserver_port, job_id, reduce_task_id)
	return (map_task_details, reduce_task_details)

def main(argv):
        parser = argparse.ArgumentParser(description='Job ID')
        parser.add_argument('-j', '--job_id', metavar='JOB_ID', help='an integer for the accumulator')

        args = parser.parse_args()

        export_job_info(args.job_id)

        sys.exit(0)


if __name__ == "__main__":
        main(sys.argv[1:])
