import requests, json, time
import sys
import argparse
import mycluster
import datetime

import numpy

import node_list
import mylog
import myinfo

def get_history_server():
	hs = myinfo.HistoryServer(node_list.historyserver['host'], node_list.historyserver['port'])
	return hs

def parse_job_id_list(aList):
	job_id_list = []
        for object in aList:
                if type(object) is dict:
                        job_ids = mylog.lookup_job_ids(object['job_log'])
                        job_id_list.extend(job_ids)
                if type(object) is str:
                        job_id_list.append(object)
	return job_id_list

def report_job_analysis(aList, output_file):
	job_id_list = parse_job_id_list(aList)

        task_detail_list = {}
        hs = get_history_server()

        fd = open(output_file, "w")
	fd.write("job_id,job_name,num_maps,num_reduces,elapsed_time")
	fd.write(",map_elapsed_time_mean,map_elapsed_time_std")
	fd.write(",map_elapsed_time_min,map_elpased_time_max")
	fd.write(",map_flow_in_mean,map_flow_in_std")
	fd.write(",map_flow_out_mean,map_flow_out_std")
	fd.write(",map_waiting_time_mean,map_waiting_time_std")
	fd.write(",reduce_elapsed_time_mean,reduce_elapsed_time_std")
	fd.write(",reduce_elapsed_time_min,reduce_elapsed_time_max")
	fd.write(",reduce_flow_in_mean,reduce_flow_in_std")
	fd.write(",reduce_flow_out_mean,reduce_flow_out_std")
	fd.write(",reduce_waiting_time_mean,reduce_waiting_time_std")
	fd.write("\n")

        for job_id in job_id_list:
                job_detail = hs.get_job_detail(job_id)
                task_list = hs.get_task_list(job_id)
		map_flow_in = []
		map_flow_out = []
		map_elapsed_time = []
		map_waiting_time = []
		reduce_flow_in = []
		reduce_flow_out = []
		reduce_elapsed_time = []
		reduce_waiting_time = []
		if job_detail is None or task_list is None:
			continue
                for task_id in task_list:
                        task_detail = hs.get_task_flow_detail(job_id, task_id)
			if "m" in task_id:
				map_flow_in.append(task_detail['flow_in'])
				map_flow_out.append(task_detail['flow_out'])
				map_elapsed_time.append(task_detail['elapsedTime'])
				map_waiting_time.append(task_detail['waitingTime']/1000000000.0)	
			elif "r" in task_id:
				reduce_flow_in.append(task_detail['flow_in'])
                                reduce_flow_out.append(task_detail['flow_out'])
				reduce_elapsed_time.append(task_detail['elapsedTime'])
				reduce_waiting_time.append(task_detail['waitingTime']/1000000000.0)

		
		fd.write("%s,%s,%s,%s" % (job_id, job_detail['name'], job_detail['mapsTotal'], job_detail['reducesTotal']))
		fd.write(",%s" % (job_detail['finishTime']-job_detail['startTime']))
		fd.write(",%s,%s" % (numpy.mean(map_elapsed_time), numpy.std(map_elapsed_time)))
		fd.write(",%s,%s" % (numpy.min(map_elapsed_time), numpy.max(map_elapsed_time)))
		fd.write(",%s,%s" % (numpy.mean(map_flow_in), numpy.std(map_flow_in)))
		fd.write(",%s,%s" % (numpy.mean(map_flow_out), numpy.std(map_flow_out)))
		fd.write(",%s,%s" % (numpy.mean(map_waiting_time), numpy.std(map_waiting_time)))
		fd.write(",%s,%s" % (numpy.mean(reduce_elapsed_time), numpy.std(reduce_elapsed_time)))
		if len(reduce_elapsed_time) != 0:
			fd.write(",%s,%s" % (numpy.min(reduce_elapsed_time), numpy.max(reduce_elapsed_time)))
		else:
			fd.write(",%s,%s" % (0, 0))
		fd.write(",%s,%s" % (numpy.mean(reduce_flow_in), numpy.std(reduce_flow_in)))
		fd.write(",%s,%s" % (numpy.mean(reduce_flow_out), numpy.std(reduce_flow_out)))
		fd.write(",%s,%s" % (numpy.mean(reduce_waiting_time), numpy.std(reduce_waiting_time)))
		fd.write("\n")
			#numpy.min(reduce_elapsed_time), numpy.max(reduce_elapsed_time), \
			#numpy.mean(reduce_waiting_time), numpy.std(reduce_waiting_time)
        fd.flush()
        fd.close()

def report_flow_demand_by_jobs(setting_list, output_file):
	job_id_list = []
        for setting in setting_list:
                job_ids = mylog.lookup_job_ids(setting['job_log'])
                job_id_list.extend(job_ids)

	task_detail_list = {}
	hs = get_history_server()

	fd = open(output_file, "w")
        fd.write("task,id,type,elpased_time,flow_in,flow_out\n")

	for job_id in job_id_list:
		job_detail = hs.get_job_detail(job_id)
		task_list = hs.get_task_list(job_id)
		for task_id in task_list:
			task_detail = hs.get_task_flow_detail(job_id, task_id)
			fd.write("%s,%s,%s,%s,%s,%s\n" % (job_detail["name"], task_id, task_detail['type'], task_detail['elapsedTime'], task_detail['flow_in'], task_detail['flow_out']))
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

def report_weak_scaling_by_jobs(setting_list, output_file):
	fd = open(output_file, "w")
        fd.write("job,num_slots,problem_size,elpased_time,sizeup,efficiency\n")
	
	hs = get_history_server()
	job_id_list = []
	job_record = {}
	for setting in setting_list:
		try:
			job = setting['job']
			job_id = mylog.lookup_job_ids(setting['job_log'])[0]
			job_size = setting['job_size']
			job_elapsed_time = hs.get_job_elapsed_time(job_id)
			job_num_slots = job_size/64
			if job not in job_record:
				job_record[job] = {}
			job_record[job][job_num_slots] = [job_size, job_elapsed_time]
		except:
			print "Unable to get the elapsed time for job:", job_id
	print job_record

	for (job, record) in job_record.iteritems():
		for num_core in sorted(record.keys()):
			job_size = record[num_core][0]
			job_elapsed_time = record[num_core][1]
			sizeup = (job_size / record[1][0]) * (record[1][1] / job_elapsed_time)
			efficiency = sizeup/num_core
			record[num_core].append(sizeup)
			record[num_core].append(efficiency)
			fd.write("%s,%s,%s,%s,%s,%s\n" % (job, num_core, job_size, job_elapsed_time, sizeup, efficiency))	
	fd.flush()
	fd.close()
		
def report_progress_timeline_by_jobs(setting_list, output_file):
	job_id_list = []
        for setting in setting_list:
                job_ids = mylog.lookup_job_ids(setting['job_log'])
                job_id_list.extend(job_ids)
        create_progress_timeline(get_history_server(), job_id_list, output_file)

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

def report_task_detail_timeline_by_jobs(setting_list, output_file):
	job_id_list = []
        for setting in setting_list:
                job_ids = mylog.lookup_job_ids(setting['job_log'])
                job_id_list.extend(job_ids)
        create_task_detail_timeline(get_history_server(), job_id_list, output_file)

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
		task_id_list = hs.get_task_list(job_id)
		if task_id_list is None:
			continue
		for task_id in task_id_list:
			task_detail = hs.get_task_detail(job_id, task_id)
			task_list[task_id] = task_detail

	mapWaitingTime = {}
	reduceWaitingTime = {}
	startTime = int(min([value['startTime'] for (key, value) in task_list.iteritems()]))
	endTime = int(max([value['finishTime'] for (key, value) in task_list.iteritems()]))

	for t in range(startTime, endTime):
		mapWaitingTime[t] = 0.0
		reduceWaitingTime[t] = 0.0
	
	for task_id in task_list.keys():
		if "waitingTime" not in task_list[task_id]:
			print "missing waiting time counter: %s" % task_id
			continue
		for t in range(int(task_list[task_id]['startTime']), int(task_list[task_id]['finishTime'])):
			if 'm' in task_id: 
				mapWaitingTime[t] += task_list[task_id]['waitingTime']/1000000000.0 / (task_list[task_id]['finishTime'] - task_list[task_id]['startTime'])
			elif 'r' in task_id:
				reduceWaitingTime[t] += task_list[task_id]['waitingTime']/1000000000.0 / (task_list[task_id]['finishTime'] - task_list[task_id]['startTime'])

	f.write("time,map_waiting_time,reduce_waiting_time,total_waiting_time\n")
	for t in range(startTime, endTime):
		f.write("%s,%s,%s,%s\n" % (datetime.datetime.fromtimestamp(t).strftime('%Y-%m-%d %H:%M:%S'), mapWaitingTime[t], reduceWaitingTime[t], mapWaitingTime[t]+reduceWaitingTime[t])) 
	f.close()


def create_task_timeline(hs, jobs, output_file):
        (task_job_mapping, task_job_id_mapping, map_list, reduce_list, mapStartTime, mapEndTime, reduceStartTime, reduceEndTime, reduceShuffleTime, reduceMergeTime) = hs.get_time_counters(jobs)

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
                #f.write("%s,%s,%s,%s,%s\n" % (t - startTime, runningMaps[t], shufflingReduces[t], mergingReduces[t], runningReduces[t]))
		timestamp = datetime.datetime.fromtimestamp(t).strftime('%Y-%m-%d %H:%M:%S')
		f.write("%s,%s,%s,%s,%s\n" % (timestamp, runningMaps[t], shufflingReduces[t], mergingReduces[t], runningReduces[t]))
        f.close()


def create_flow_timeline(hs, jobs, output_file):
	flow_in_table = {
		"terasort": 6.2,
		"grep": 13.8,
		"word count": 3.6,
		"custommap": 6.8,
		"default": 5,
	}
	flow_out_table = {
		"terasort": 9.5,
                "grep": 0,
                "word count": 3.8,
		"custommap": 0,
                "default": 0,
        }

        (task_job_mapping, task_job_id_mapping, ap_list, reduce_list, mapStartTime, mapEndTime, reduceStartTime, reduceEndTime, reduceShuffleTime, reduceMergeTime) = hs.get_time_counters(jobs)

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
                #f.write("%s,%s,%s,%s,%s,%s,%s\n" % (t - startTime, runningMaps[t], shufflingReduces[t], mergingReduces[t], runningReduces[t], mapFlowIn[t], reduceFlowOut[t]))
		timestamp = datetime.datetime.fromtimestamp(t).strftime('%Y-%m-%d %H:%M:%S')
		f.write("%s,%s,%s,%s,%s,%s,%s\n" % (timestamp, runningMaps[t], shufflingReduces[t], mergingReduces[t], runningReduces[t], mapFlowIn[t], reduceFlowOut[t])
)
        f.close()

def create_task_detail_timeline(hs, jobs, output_file):
        (task_job_mapping, task_job_id_mapping, map_list, reduce_list, mapStartTime, mapEndTime, reduceStartTime, reduceEndTime, reduceShuffleTime, reduceMergeTime) = hs.get_time_counters(jobs)

        startTime = min(
                reduce(min, mapStartTime.values()),
                reduce(min, reduceStartTime.values()))
        endTime = max(
                reduce(max, mapEndTime.values()),
                reduce(max, reduceEndTime.values()))

	job_timeline = {}
	for job in jobs:
		runningMaps = {}
        	shufflingReduces = {}
        	mergingReduces = {}
        	runningReduces = {}
        	for t in range(startTime, endTime):
               		runningMaps[t] = 0
                	shufflingReduces[t] = 0
                	mergingReduces[t] = 0
                	runningReduces[t] = 0
		job_timeline[job] = [runningMaps, shufflingReduces, mergingReduces, runningReduces]

	for mapper in mapStartTime.keys():
                for t in range(mapStartTime[mapper], mapEndTime[mapper]):
			job_timeline[task_job_id_mapping[mapper]][0][t] += 1
        for reducer in reduceStartTime.keys():
                for t in range(reduceStartTime[reducer], reduceShuffleTime[reducer]):
			job_timeline[task_job_id_mapping[mapper]][1][t] += 1
                for t in range(reduceShuffleTime[reducer], reduceMergeTime[reducer]):
			job_timeline[task_job_id_mapping[mapper]][2][t] += 1
                for t in range(reduceMergeTime[reducer], reduceEndTime[reducer]):
			job_timeline[task_job_id_mapping[mapper]][3][t] += 1

        f = open(output_file, "w")
	f.write("time")
	for job in jobs:
		f.write(",%s_map,%s_shuffle,%s_merge,%s_reduce" % (job, job, job, job))
	f.write(",total_map,total_shuffle,total_merge,total_reduce\n")
        for t in range(startTime, endTime):
                timestamp = datetime.datetime.fromtimestamp(t).strftime('%Y-%m-%d %H:%M:%S')
		f.write(timestamp)
		total_map = 0
		total_shuffle = 0
		total_merge = 0
		total_reduce = 0
		for job in jobs:
			timeline = job_timeline[job]
			total_map += timeline[0][t]
			total_shuffle += timeline[1][t]
			total_merge += timeline[2][t]
			total_reduce += timeline[3][t]
			f.write(",%s,%s,%s,%s" % (timeline[0][t], timeline[1][t], timeline[2][t], timeline[3][t]))
		f.write(",%s,%s,%s,%s\n" % (total_map, total_shuffle, total_merge, total_reduce))
			
        f.close()

def create_progress_timeline(hs, jobs, output_file, aggregate=False):
	(task_job_mapping, task_job_id_mapping, map_list, reduce_list, mapStartTime, mapEndTime, reduceStartTime, reduceEndTime, reduceShuffleTime, reduceMergeTime) = hs.get_time_counters(jobs)

	job_name_mapping = {}
	if aggregate:
		for job in jobs:
			job_detail = hs.get_job_detail(job)
			job_name_mapping[job] = job_detail['name']
		

        startTime = min(
                reduce(min, mapStartTime.values()),
                reduce(min, reduceStartTime.values()))
        endTime = max(
                reduce(max, mapEndTime.values()),
                reduce(max, reduceEndTime.values()))

	# initialize
	total_timeline = {}
        job_timeline = {}
	for t in range(startTime, endTime):
		total_timeline[t] = 0
        for job in jobs:
		throughput = {}
                for t in range(startTime, endTime):
			throughput[t] = 0
		job_key = job_name_mapping[job] if aggregate else job
                job_timeline[job_key] = throughput

	# value extraction
        for mapper in mapStartTime.keys():
		job_key = job_name_mapping[task_job_id_mapping[mapper]] if aggregate else task_job_id_mapping[mapper]
                task_detail = hs.get_task_detail(task_job_id_mapping[mapper], mapper)
		bytes_processed = task_detail['BYTES_READ'] if "MAP" == task_detail['type'] else task_detail['BYTES_WRITTEN']
		bytes_per_second = bytes_processed / float(mapEndTime[mapper] - mapStartTime[mapper])
                for t in range(mapStartTime[mapper], mapEndTime[mapper]):
                        job_timeline[job_key][t] += bytes_per_second

        for reducer in reduceStartTime.keys():
		period = reduceEndTime[reducer] - reduceStartTime[reducer]
		if period == 0:
			continue
		job_key = job_name_mapping[task_job_id_mapping[reducer]] if aggregate else task_job_id_mapping[reducer]
                task_detail = hs.get_task_detail(task_job_id_mapping[reducer], reducer)
                bytes_processed = task_detail['BYTES_READ'] if "MAP" == task_detail['type'] else task_detail['BYTES_WRITTEN']
		bytes_per_second = bytes_processed / float(reduceEndTime[reducer] - reduceStartTime[reducer])
                for t in range(reduceMergeTime[reducer], reduceEndTime[reducer]):
			job_timeline[job_key][t] += bytes_per_second

	#accumulate
	for (job_key, timeline) in job_timeline.iteritems():
		for t in range(startTime, endTime-1):
			timeline[t+1] += timeline[t]
	for (job_key, timeline) in job_timeline.iteritems():
		for t in range(startTime, endTime):
			total_timeline[t] += timeline[t]

        f = open(output_file, "w")
        f.write("time")
        for job_key in sorted(job_timeline.keys()):
                f.write(",%s" % job_key)
	f.write(",total_throughput")
	for job_key in sorted(job_timeline.keys()):
                f.write(",%s" % job_key)
        f.write(",total_progress")
	f.write("\n")
	
        for t in range(startTime, endTime):
                timestamp = datetime.datetime.fromtimestamp(t).strftime('%Y-%m-%d %H:%M:%S')
                f.write(timestamp)
		for job_key in sorted(job_timeline.keys()):
			throughput_timeline = job_timeline[job_key]
			throughput = throughput_timeline[t]	
                        f.write(",%s" % throughput)
		f.write(",%s" %  total_timeline[t])
		for job_key in sorted(job_timeline.keys()):
                        throughput_timeline = job_timeline[job_key]
                        progress = throughput_timeline[t] / float(throughput_timeline[endTime-1])
                        f.write(",%s" % progress)
                f.write(",%s" %  (total_timeline[t]/total_timeline[endTime-1]))
		f.write("\n")

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
