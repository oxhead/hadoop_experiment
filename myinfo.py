#!/usr/bin/python

import sys, argparse, requests, json
import mylog
import mycluster
from node_list import historyserver
import numpy

historyserver_host = historyserver['host']
historyserver_port = historyserver['port']


class HistoryServer():
	def __init__(self, host, port):
		self.host = host
		self.port = port

	def get_query_url(self):
		url = "http://%s:%s/ws/v1/history/mapreduce" % (self.host, self.port);
		return url

	def get_job_url(self, job_id):
		url = "%s/jobs/%s" % (self.get_query_url(), job_id);
		return url

	def get_task_url(self, job_id, task_id):
                url = "%s/tasks/%s" % (self.get_job_url(job_id), task_id);
		return url

	def get_time_counters(self, job_list):
		
		mapStartTime = {}
		mapEndTime = {}
		reduceStartTime = {}
		reduceShuffleTime = {}
		reduceMergeTime = {}
		reduceEndTime = {}
		reduceBytes = {}

		map_list = []
		reduce_list = []
	
		task_job_mapping = {}

		for job_id in job_list:	
			task_list = self.get_task_list(job_id)
			job_detail = self.get_job_detail(job_id)
			job_name = job_detail["name"]
			for task_id in task_list:
				task_job_mapping[task_id] = job_name
				task_counter = self.get_task_time_counters(job_id, task_id)
				(taskMapStartTime, taskMapEndTime, taskReduceStartTime, taskReduceEndTime, taskReduceShuffleTime, taskReduceMergeTime) = self.get_task_time_counters(job_id, task_id)
				if "m" in task_id:
					mapStartTime[task_id] = taskMapStartTime
					mapEndTime[task_id] = taskMapEndTime
					map_list.append(task_id)
				elif "r" in task_id:
					reduceStartTime[task_id] = taskReduceStartTime
					reduceEndTime[task_id] = taskReduceEndTime
					reduceShuffleTime[task_id] = taskReduceShuffleTime
					reduceMergeTime[task_id] = taskReduceMergeTime 
					reduce_list.append(task_id)
		return (task_job_mapping, map_list, reduce_list, mapStartTime, mapEndTime, reduceStartTime, reduceEndTime, reduceShuffleTime, reduceMergeTime)
		
	# at attempt level
	# only second accuracy
	def get_task_time_counters(self, job_id, task_id):
		mapStartTime = None
       		mapEndTime = None
        	reduceStartTime = None
        	reduceShuffleTime = None
        	reduceMergeTime = None
        	reduceEndTime = None

                attempt_list_url = self.get_task_url(job_id, task_id) + "/attempts"
                attempt_list_json = requests.get(attempt_list_url)
                for attempt in attempt_list_json.json()["taskAttempts"]["taskAttempt"]:
                        attempt_url = attempt_list_url + "/" + attempt["id"]
                        attempt_json = requests.get(attempt_url)
			json_content = attempt_json.json()
                        attempt_id = json_content["taskAttempt"]["id"]
                        attempt_type = json_content["taskAttempt"]["type"]
                        attempt_state = json_content["taskAttempt"]["state"]
                        attempt_startTime = json_content["taskAttempt"]["startTime"]
                        attempt_endTime = json_content["taskAttempt"]["finishTime"]
                        if attempt_type == "MAP":
                                mapStartTime = int(attempt_startTime)/1000;
                                mapEndTime  = int(attempt_endTime)/1000;
                        elif attempt_type == "REDUCE":
                                attempt_shuffleTime = json_content["taskAttempt"]["shuffleFinishTime"]
                                attempt_mergeTime = json_content["taskAttempt"]["mergeFinishTime"]
                                reduceStartTime  = int(attempt_startTime)/1000
                                reduceEndTime = int(attempt_endTime)/1000
                                reduceShuffleTime = int(attempt_shuffleTime)/1000
                                reduceMergeTime = int(attempt_mergeTime)/1000
		return (mapStartTime, mapEndTime, reduceStartTime, reduceEndTime, reduceShuffleTime, reduceMergeTime)
	
	# only at task level, but not task attempt level
	def get_task_counters(self, job_id, task_id):
		task_url = self.get_task_url(job_id, task_id)
		task_json_response = requests.get(task_url)
		task_json = task_json_response.json()
		if task_json["task"]["state"] != "SUCCEEDED":
			return;

		counters = {}
		counters['type'] = task_json["task"]["type"]
		counters['startTime'] = task_json["task"]["startTime"]/1000.0
		counters['finishTime'] = task_json["task"]["finishTime"]/1000.0
		
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
					elif counter_group['counterGroupName'] == "my":
						if counter['name'] == "time_read_in":
							counters['waitingTime'] = counter['value']
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


	# time_start: sec
	# time_end: sec, e.g. time.time()
	def get_job_list(self, time_start=None, time_end=None):
		time_start = time_start if time_start is not None else ""
		time_end = time_end if time_end is not None else ""
		job_list_url = "%s/jobs" % self.get_query_url()
		job_list_query_url = "%s?startedTimeBegin=%s&finishTimeEnd=%s" % (job_list_url, time_start*1000, time_end*1000)
		jobs = []
		job_list_json = requests.get(job_list_query_url)
		try:
			jobs = job_list_json.json()["jobs"]["job"]
		except:
			print "Unable to retrieve job lists at", job_list_query_url
		job_list = []
		for job in jobs:
			if job["state"] == "SUCCEEDED":
				job_list.append(job["id"])

		return job_list

	def get_task_list(self, job_id, type=None):
		tasks_url = "%s/tasks" % self.get_job_url(job_id);
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
		task_list = []
		if type is None or type.lower() == "map":
			for task in map_task_list:
				task_list.append(task)
		if type is None or type.lower() == "reduce":
			for task in reduce_task_list:
				task_list.append(task)
		
		return task_list

	def get_task_detail(self, job_id, task_id):
		task_detail = self.get_task_counters(job_id, task_id)
		return task_detail

	def get_job_detail(self, job_id):
		job_url = "http://%s:%s/ws/v1/history/mapreduce/jobs/%s" % (historyserver_host, historyserver_port, job_id);
		job_json_response = requests.get(job_url)
		job_json = job_json_response.json()
		if job_json["job"]["state"] != "SUCCEEDED":
			return;

		counters = {}
		counters['name'] = job_json["job"]["name"]
		counters['startTime'] = job_json["job"]["startTime"]/1000.0
		counters['finishTime'] = job_json["job"]["finishTime"]/1000.0
		return counters

	def get_job_elapsed_time(self, job_id):
		job_detail = self.get_job_detail(job_id)
		return job_detail['finishTime'] - job_detail['startTime']	

	def get_elapsed_time(self, job_id_list):
		start_time = sys.maxint
		finish_time = -1
		for job_id in job_id_list:
			job_detail = slef.get_job_detail(job_id)
			start_time = job_detail['startTime'] if job_detail['startTime'] < start_time else start_time
			finish_time = job_detail['finishTime'] if job_detail['finishTime'] > finish_time else finish_time
		return (start_time, finish_time)

	def get_task_flow_detail(self, job_id, task_id):
		task_detail = self.get_task_detail(job_id, task_id)
		elapsed_task_time = task_detail['finishTime'] - task_detail['startTime']
		task_detail['elapsedTime'] = elapsed_task_time
		
		if task_detail['type'] == "MAP":
			task_detail['input_size'] = int(task_detail['BYTES_READ'])
			task_detail['output_size'] = int(task_detail['MAP_OUTPUT_MATERIALIZED_BYTES'])
			task_detail['flow_in'] = task_detail['input_size'] / (1024.0*1024.0) / task_detail['elapsedTime']
			task_detail['flow_out'] = task_detail['output_size'] / (1024.0*1024.0) / task_detail['elapsedTime']
		elif task_detail['type'] == "REDUCE":
			task_detail['input_size'] = int(task_detail['REDUCE_SHUFFLE_BYTES'])
			task_detail['output_size'] = int(task_detail['BYTES_WRITTEN'])
			task_detail['flow_in'] = task_detail['input_size'] / (1024.0*1024.0) / task_detail['elapsedTime']
			task_detail['flow_out'] = task_detail['output_size'] / (1024.0*1024.0) / task_detail['elapsedTime']
		return task_detail

def main(argv):
        parser = argparse.ArgumentParser(description='Job ID')
        parser.add_argument('--host', required=True, help="The host adress of the history server")
        parser.add_argument('--port', default="19888", required=False, help="The port of the history server. The default port number is 19888")

        args = parser.parse_args()

	hs = HistoryServer(args.host, args.port)
	#job_list = hs.get_job_list()
	#print job_list
	output = hs.get_task_flow_detail("job_1394993376002_0004", "task_1394993376002_0004_m_000000")
	print output
        sys.exit(0)


if __name__ == "__main__":
        main(sys.argv[1:])
