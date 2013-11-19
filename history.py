import requests, json, time
import sys
import argparse

def fetch_history(host, port, time_start, time_end, output_file):
	job_list_url = "http://%s:%s/ws/v1/history/mapreduce/jobs" % (host, port)
        job_list_query_url = "%s?startedTimeBegin=%s&startedTimeEnd=%s" % (job_list_url, time_start, time_end)
	job_list_json = requests.get(job_list_query_url)
	jobs = job_list_json.json()["jobs"]["job"]
	job_list = []
	for job in jobs:
		if job["state" == "SUCCEEDED"]:
			job_list.append(job["id"])
	
	fetch_jobs(host, port, job_list, output_file)
	
def fetch_jobs(host, port, jobs, output_file):
	f = open(output_file, "w")
	job_list_url = "http://%s:%s/ws/v1/history/mapreduce/jobs" % (host, port)
	
	mapStartTime = {}
	mapEndTime = {}
	reduceStartTime = {}
	reduceShuffleTime = {}
	reduceMergeTime = {}
	reduceEndTime = {}
	reduceBytes = {}

	map_list = []
	reduce_list = []

	for job_id in jobs:
		job_url = "%s/%s" % (job_list_url, job_id)
		job_json = requests.get(job_url)
		#print job_json.json()
		job = job_json.json()["job"]
		
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
			task_list_url = task_list_per_job_url + "/" + task["id"]
			#print task_list_url
			task_list_json = requests.get(task_list_url)
			attempt_list_url = task_list_url + "/attempts"
			attempt_list_json = requests.get(attempt_list_url)
			for attempt in attempt_list_json.json()["taskAttempts"]["taskAttempt"]:
				attempt_url = attempt_list_url + "/" + attempt["id"]
				attempt_json = requests.get(attempt_url)
				attempt_id = attempt_json.json()["taskAttempt"]["id"]
                        	attempt_type = attempt_json.json()["taskAttempt"]["type"]
                        	attempt_state = attempt_json.json()["taskAttempt"]["state"]
                        	attempt_startTime = attempt_json.json()["taskAttempt"]["startTime"]
				attempt_endTime = attempt_json.json()["taskAttempt"]["finishTime"]
				#print attempt_url
				if attempt_type == "MAP":
					mapStartTime[attempt_id] = int(attempt_startTime)/1000;
					mapEndTime[attempt_id] = int(attempt_endTime)/1000;
					map_list.append(attempt_id)
				elif attempt_type == "REDUCE":
					attempt_shuffleTime = attempt_json.json()["taskAttempt"]["shuffleFinishTime"]
					attempt_mergeTime = attempt_json.json()["taskAttempt"]["mergeFinishTime"]
					reduceStartTime[attempt_id] = int(attempt_startTime)/1000
                                	reduceEndTime[attempt_id] = int(attempt_endTime)/1000
					reduceShuffleTime[attempt_id] = int(attempt_shuffleTime)/1000
					reduceMergeTime[attempt_id] = int(attempt_mergeTime)/1000
					reduce_list.append(attempt_id)

	runningMaps = {}
	shufflingReduces = {}
	mergingReduces = {}
	runningReduces = {}
	startTime = min(
		reduce(min, mapStartTime.values()),
                reduce(min, reduceStartTime.values()))
	endTime = max(
		reduce(max, mapEndTime.values()),
           	reduce(max, reduceEndTime.values()))

	#print startTime, endTime
	for t in range(startTime, endTime):
		#print t
        	runningMaps[t] = 0
        	shufflingReduces[t] = 0
        	mergingReduces[t] = 0
        	runningReduces[t] = 0

	for mapper in mapStartTime.keys():
        	for t in range(mapStartTime[mapper], mapEndTime[mapper]):
                	runningMaps[t] += 1
	for reducer in reduceStartTime.keys():
        	for t in range(reduceStartTime[reducer], reduceShuffleTime[reducer]):
                	shufflingReduces[t] += 1
        	for t in range(reduceShuffleTime[reducer], reduceMergeTime[reducer]):
               		mergingReduces[t] += 1
        	for t in range(reduceMergeTime[reducer], reduceEndTime[reducer]):
                	runningReduces[t] += 1

	print "Mapper=%s, Reducer=%s" % (len(map_list), len(reduce_list))

	f.write("time,maps,shuffle,merge,reduce\n")
	for t in range(startTime, endTime):
		f.write("%s,%s,%s,%s,%s\n" % (t - startTime, runningMaps[t], shufflingReduces[t], mergingReduces[t], runningReduces[t])) 
	f.close()

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

	fetch_history(args.host, args.port, "", "", args.file)

if __name__ == "__main__":
        main(sys.argv[1:])
