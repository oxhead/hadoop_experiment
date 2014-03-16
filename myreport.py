import requests, json, time
import sys
import argparse
import mycluster
import datetime

import node_list
import mylog
import myinfo

def get_history_server():
	hs = myinfo.HistoryServer(node_list.historyserver['host'], node_list.historyserver['port'])
	return hs

def report_flow_demand_by_jobs(setting_list, output_file):
	job_id_list = []
        for setting in setting_list:
                job_ids = mylog.lookup_job_ids(setting['job_log'])
                job_id_list.extend(job_ids)

	task_detail_list = {}
	hs = get_history_server()

	fd = open(output_file, "w")
        print >> fd, "task", "id", "type", "elpased_time", "flow_in", "flow_out",

        job_list = hs.get_job_list()
	for job_id in job_id_list:
		job_detail = hs.get_job_detail(job_id)
		task_list = hs.get_task_list(job_id)
		for task_id in task_list:
			task_detail = hs.get_task_flow_detail(job_id, task_id)
			print >> fd, job_detail["name"], task_id, task_detail['type'], task_detail['elapsedTime'], task_detail['flow_in'], task_detail['flow_out']
	fd.flush()
	fd.close()


# support flow-in only
def report_computing_flow_capability_by_jobs(setting_list, output_file):
	fd = open(output_file, "w")
        print >> fd, "job_size", "num_tasks", "elpased_time", "flow_in", "flow_out"
	
	for i in range(len(setting_list)):
		setting = setting_list[i]	
		job_id = mylog.lookup_job_ids(setting['job_log'])[0]
		job_size = setting['job_size']
		hs = get_history_server()

		totalBytes = 0
		startTime = sys.maxint
		finishTime = -1
		job_detail = hs.get_job_detail(job_id)
		task_list = hs.get_task_list(job_id, "map")
		for task_id in task_list:
			task_detail = hs.get_task_counters(job_id, task_id)
			startTime = task_detail["startTime"] if task_detail["startTime"] < startTime else startTime
			finishTime = task_detail["finishTime"] if task_detail["finishTime"] > finishTime else finishTime
			totalBytes = totalBytes + task_detail['BYTES_READ']
		
		flow_in = (totalBytes/1024/1024) / float(finishTime - startTime)		
		flow_out = 0
		print >> fd, job_size, len(task_list), finishTime - startTime, flow_in, flow_out
		fd.flush()
        fd.close()

# support flow-out only
def report_storage_flow_capability_by_jobs(setting_list, num_nodes, output_file):
        fd = open(output_file, "w")
        print >> fd, "num_nodes", "num_tasks", "elpased_time", "flow_in", "flow_out"

	totalBytes = 0
        startTime = sys.maxint
        finishTime = -1
        for i in range(len(setting_list)):
                setting = setting_list[i]
                job_id = mylog.lookup_job_ids(setting['job_log'])[0]
                job_size = setting['job_size']
                hs = get_history_server()
                job_detail = hs.get_job_detail(job_id)
                task_list = hs.get_task_list(job_id, "map")
                for task_id in task_list:
                        task_detail = hs.get_task_counters(job_id, task_id)
                        startTime = task_detail["startTime"] if task_detail["startTime"] < startTime else startTime
                        finishTime = task_detail["finishTime"] if task_detail["finishTime"] > finishTime else finishTime
                        totalBytes = totalBytes + task_detail['BYTES_READ']

	flow_in = 0
	flow_out = (totalBytes/1024/1024) / float(finishTime - startTime)
        print >> fd, num_nodes, len(task_list), finishTime - startTime, flow_in, flow_out
        fd.flush()
        fd.close()
		

def report_waiting_time(time_start, time_end, output_file):
	hs = get_history_server()
	job_list = hs.get_job_list(time_start, time_end)
	create_waiting_time(hs, job_list, output_file)

def report_waiting_time_by_jobs(setting_list, output_file):
	job_id_list = []
	for setting in setting_list:
		job_ids = mylog.lookup_job_ids(setting['job_log'])
		job_id_list.extend(job_ids)
	create_waiting_time(get_history_server(), job_id_list, output_file)

def report_task_timeline(time_start, time_end, output_file):
	hs = get_history_server()
        job_list = hs.get_job_list(time_start, time_end)
        create_task_timeline(hs, job_list, output_file)

def report_task_timeline_by_jobs(setting_list, output_file):
        job_id_list = []
        for setting in setting_list:
                job_ids = mylog.lookup_job_ids(setting['job_log'])
                job_id_list.extend(job_ids)
        create_task_timeline(get_history_server(), job_id_list, output_file)

def report_flow_timeline(time_start, time_end, output_file):
        hs = get_history_server()
        job_list = hs.get_job_list(time_start, time_end)
        create_flow_timeline(hs, job_list, output_file)

def report_flow_timeline_by_jobs(setting_list, output_file):
        job_id_list = []
        for setting in setting_list:
                job_ids = mylog.lookup_job_ids(setting['job_log'])
                job_id_list.extend(job_ids)
        create_flow_timeline(get_history_server(), job_id_list, output_file)

def query_waiting_time(host, port, output_file):
	hs = myinfo.HistoryServer(host, port)
	job_list = hs.get_job_list()
	create_waiting_time(hs, job_list, output_file)

def query_task_timeline(host, port, output_file):
        hs = myinfo.HistoryServer(host, port)
        job_list = hs.get_job_list()
        create_task_timeline(hs, job_list, output_file)

def query_flow_timeline(host, port, output_file):
        hs = myinfo.HistoryServer(host, port)
        job_list = hs.get_job_list()
        create_flow_timeline(hs, job_list, output_file)	

def create_waiting_time(hs, jobs, output_file):
	f = open(output_file, "w")
	
	task_list = {}

	for job_id in jobs:
		task_id_list = hs.get_task_list(job_id, "map")
		for task_id in task_id_list:
			task_detail = hs.get_task_detail(job_id, task_id)
			task_list[task_id] = task_detail

	waitingTime = {}
	waitingPercentage = {}
	startTime = int(min([value['startTime'] for (key, value) in task_list.iteritems()]))
	endTime = int(max([value['finishTime'] for (key, value) in task_list.iteritems()]))

	for t in range(startTime, endTime):
		waitingTime[t] = 0.0
	
	for task_id in task_list.keys():
		if "waitingTime" not in task_list[task_id]:
			print "missing waiting time counter: %s" % task_id
			continue
		for t in range(int(task_list[task_id]['startTime']), int(task_list[task_id]['finishTime'])):
			waitingTime[t] += task_list[task_id]['waitingTime']/1000000000.0 / (task_list[task_id]['finishTime'] - task_list[task_id]['startTime'])

	f.write("time,waiting_time\n")
	for t in range(startTime, endTime):
		f.write("%s,%s\n" % (datetime.datetime.fromtimestamp(t).strftime('%Y-%m-%d %H:%M:%S'), waitingTime[t])) 
	f.close()


def create_task_timeline(hs, jobs, output_file):
        (task_job_mapping, map_list, reduce_list, mapStartTime, mapEndTime, reduceStartTime, reduceEndTime, reduceShuffleTime, reduceMergeTime) = hs.get_time_counters(jobs)

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

	f = open(output_file, "w")	
        f.write("time,maps,shuffle,merge,reduce\n")
        for t in range(startTime, endTime):
                f.write("%s,%s,%s,%s,%s\n" % (t - startTime, runningMaps[t], shufflingReduces[t], mergingReduces[t], runningReduces[t]))
        f.close()


def create_flow_timeline(hs, jobs, output_file):
	flow_in_table = {
		"terasort": 1,
		"grep": 0.4,
		"word count": 1.3,
		"custommap": 2,
		"default": 1,
	}
	flow_out_table = {
		"terasort": 1,
                "grep": 0.4,
                "word count": 1.3,
		"custommap": 3,
                "default": 1,
        }

        (task_job_mapping, map_list, reduce_list, mapStartTime, mapEndTime, reduceStartTime, reduceEndTime, reduceShuffleTime, reduceMergeTime) = hs.get_time_counters(jobs)

        runningMaps = {}
        shufflingReduces = {}
        mergingReduces = {}
        runningReduces = {}
	mapFlowIn = {}
	mapFlowOut = {}
	reduceFlowIn = {}
	reduceFlowOut = {}
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
		mapFlowIn[t] = 0
		reduceFlowOut[t] = 0

        for mapper in mapStartTime.keys():
		job_name = task_job_mapping[mapper]
		r = range(mapStartTime[mapper], mapEndTime[mapper])
                for t in r:
                        runningMaps[t] += 1
			found = False
			for (job, flow) in flow_in_table.iteritems():
				if job in job_name.lower():
					mapFlowIn[t] += float(flow)/len(r)
			if not found:
                                mapFlowIn[t] += float(flow_in_table['default'])/len(r)

        for reducer in reduceStartTime.keys():
		job_name = task_job_mapping[reducer]
                for t in range(reduceStartTime[reducer], reduceShuffleTime[reducer]):
                        shufflingReduces[t] += 1
                for t in range(reduceShuffleTime[reducer], reduceMergeTime[reducer]):
                        mergingReduces[t] += 1
                for t in range(reduceMergeTime[reducer], reduceEndTime[reducer]):
                        runningReduces[t] += 1
		r = range(mapStartTime[mapper], mapEndTime[mapper])
		for t in r:
			found = False
                        for (job, flow) in flow_out_table.iteritems():
                                if job in job_name.lower():
                                        reduceFlowOut[t] += float(flow)/len(r)
					found = True
			if not found:
				reduceFlowOut[t] += float(flow_out_table['default'])/len(r)

	f = open(output_file, "w")
        f.write("time,maps,shuffle,merge,reduce,map_flow_in, reduce_flow_out\n")
        for t in range(startTime, endTime):
                f.write("%s,%s,%s,%s,%s,%s,%s\n" % (t - startTime, runningMaps[t], shufflingReduces[t], mergingReduces[t], runningReduces[t], mapFlowIn[t], reduceFlowOut[t]))
        f.close()



def main(argv):

        parser = argparse.ArgumentParser(description='Fetch Hadoop history from history server')

        parser.add_argument('--host', required=True, help="The host adress of the history server")
	parser.add_argument('--port', default="19888", required=False, help="The port of the history server. The default port number is 19888")
	parser.add_argument('-f', '--file', required=True, help="The output file")
	parser.add_argument('-t', '--type', choices=["waiting_time", "task_timeline", "flow_timeline"], help="The type of report")

        args = parser.parse_args()

	if "waiting_time" == args.type:
		query_waiting_time(args.host, args.port, args.file)
	elif "task_timeline" == args.type:
		query_task_timeline(args.host, args.port, args.file)
	elif "flow_timeline" == args.type:
		query_flow_timeline(args.host, args.port, args.file)


if __name__ == "__main__":
        main(sys.argv[1:])
