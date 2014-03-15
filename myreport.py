import requests, json, time
import sys
import argparse
import mycluster
import datetime
import mylog

def report_waiting_time(time_start, time_end, output_file):
	cluster = mycluster.load()
	fetch_history(cluster.mapreduce.getResourceManager().host, "19888", time_start, time_end, output_file)	

def report_waiting_time_by_jobs(setting_list, output_file):
	cluster = mycluster.load()
	job_id_list = []
	for setting in setting_list:
		job_ids = mylog.lookup_job_ids(setting['job_log'])
		job_id_list.extend(job_ids)
	fetch_jobs(cluster.mapreduce.getResourceManager().host, "19888", job_id_list, output_file)

	

# time_start: sec
# time_end: sec, e.g. time.time()
def fetch_history(host, port, time_start, time_end, output_file):
	#if time_start is not None and time_end is not None:
	#	print "# Start time:", time_start, datetime.datetime.fromtimestamp(time_start).strftime('%Y-%m-%d %H:%M:%S')
       	# 	print "# End time:", time_end, datetime.datetime.fromtimestamp(time_end).strftime('%Y-%m-%d %H:%M:%S')
	time_start = time_start if time_start is not None else ""
	time_end = time_end if time_end is not None else ""
	job_list_url = "http://%s:%s/ws/v1/history/mapreduce/jobs" % (host, port)
        job_list_query_url = "%s?startedTimeBegin=%s&finishTimeEnd=%s" % (job_list_url, time_start*1000, time_end*1000)
	print "Job lists ->", job_list_query_url
	job_list_json = requests.get(job_list_query_url)
	jobs = job_list_json.json()["jobs"]["job"]
	job_list = []
	for job in jobs:
		if job["state"] == "SUCCEEDED":
			job_list.append(job["id"])
	
	fetch_jobs(host, port, job_list, output_file)

	
def fetch_jobs(host, port, jobs, output_file):
	f = open(output_file, "w")
	job_list_url = "http://%s:%s/ws/v1/history/mapreduce/jobs" % (host, port)
	
	task_list = {}

	for job_id in jobs:
		job_url = "%s/%s" % (job_list_url, job_id)
		print "\t", job_url
		job_json = requests.get(job_url)
		#print job_json.json()
		try:
			job = job_json.json()["job"]
		except:
			print "Error", job_id
			continue
		
		if job["state"] != "SUCCEEDED":
			continue


		#print job
		task_list_per_job_url = job_list_url + "/" + job["id"] + "/tasks"
		task_list_per_job_json = requests.get(task_list_per_job_url)
		#print task_list_per_job_url
		if not ("tasks" in task_list_per_job_json.json()):
			print "Exception"
			continue
		#print task_list_per_job_json.json()
		#print task_list_per_job_json.json()["tasks"]['task']
		for task in task_list_per_job_json.json()["tasks"]["task"]:
			if task['type'] != "MAP":
				continue
			task_counters_url = task_list_per_job_url + "/" + task["id"] + "/counters"
			#print task_counters_url
			task_counters = requests.get(task_counters_url).json()
			for counterGroup in task_counters['jobTaskCounters']['taskCounterGroup']:
				if counterGroup['counterGroupName'] == 'my':
					#print task['id'], counterGroup['counter'][0]['value']
					task_list[task['id']] = {'startTime': int(task['startTime'])/1000, 'finishTime': int(task['finishTime'])/1000, 'waitingTime': counterGroup['counter'][0]['value']}


	waitingTime = {}
	waitingPercentage = {}
	startTime = min([value['startTime'] for (key, value) in task_list.iteritems()])
	endTime = max([value['finishTime'] for (key, value) in task_list.iteritems()])

	#print startTime, endTime
	for t in range(startTime, endTime):
		waitingTime[t] = 0.0
	
	for task_id in task_list.keys():
		for t in range(task_list[task_id]['startTime'], task_list[task_id]['finishTime']):
			waitingTime[t] += task_list[task_id]['waitingTime']/1000000 / (task_list[task_id]['finishTime'] - task_list[task_id]['startTime'])

	#print "Mapper=%s" % len(task_list)
	f.write("time,waiting_time\n")
	for t in range(startTime, endTime):
		f.write("%s,%s\n" % (datetime.datetime.fromtimestamp(t).strftime('%Y-%m-%d %H:%M:%S'), waitingTime[t])) 
	f.close()

	#print "@ Start time:", datetime.datetime.fromtimestamp(startTime).strftime('%Y-%m-%d %H:%M:%S')
        #print "@ End time:", datetime.datetime.fromtimestamp(endTime).strftime('%Y-%m-%d %H:%M:%S')

def fetch_job_info(host, port, job_id, output_file):
	f = open(output_file, "w")

	job_url = "http://%s:%s/ws/v1/history/mapreduce/jobs/%s" % (host, port, job_id)
        #print job_url
        job_json_response = requests.get(job_url)
        job_json = job_json_response.json()
        #print job_json
	job = job_json["job"]
        if job["state"] != "SUCCEEDED":
                return;
        job_name=job["name"]
        #print "@ " + job_name
        task_list_url = job_url + "/tasks"
        task_list_json_response = requests.get(task_list_url)
        task_list_json = task_list_json_response.json()
        #print task_list_url
        #print task_list_json
        for task in task_list_json["tasks"]["task"]:
		task_id = task["id"]
                attempt_url = task_list_url + "/" + task["id"] + "/attempts/" + task["successfulAttempt"]
                #print attempt_url
                attempt_json_response = requests.get(attempt_url)
                attempt_json = attempt_json_response.json()
                #print attempt_json
                attempt_type = attempt_json["taskAttempt"]["type"]
		attempt_time_start = attempt_json["taskAttempt"]["startTime"]
		attempt_time_finish = attempt_json["taskAttempt"]["finishTime"]
                attempt_time = attempt_json["taskAttempt"]["elapsedTime"]
                attempt_node = attempt_json["taskAttempt"]["nodeHttpAddress"]
		f.write("%s,%s,%s,%s,%s,%s,%s,%s\n" % (job_id, job_name, task_id, attempt_type, attempt_time_start, attempt_time_finish, attempt_time, attempt_node))

	f.close()

def main(argv):

        parser = argparse.ArgumentParser(description='Fetch Hadoop history from history server')

        parser.add_argument('--host', required=True, help="The host adress of the history server")
	parser.add_argument('--port', default="19888", required=False, help="The port of the history server. The default port number is 19888")
	parser.add_argument('-f', '--file', required=True, help="The output file")

        args = parser.parse_args()

	fetch_history(args.host, args.port, None, None, args.file)

if __name__ == "__main__":
        main(sys.argv[1:])
