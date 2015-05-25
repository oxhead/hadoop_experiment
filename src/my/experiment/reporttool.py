import requests
import json
import time
import sys
import argparse
import datetime
import numpy

from my.experiment import historytool

def get_start_time(mapStartTime, reduceStartTime):
    startTime = 0
    if len(reduceStartTime) > 0:
        startTime = min(
            reduce(min, mapStartTime.values()),
            reduce(min, reduceStartTime.values()))
    else:
        startTime = reduce(min, mapStartTime.values())
    return startTime

def get_end_time(mapEndTime, reduceEndTime):
    endTime = 0
    if len(reduceEndTime) > 0:
        endTime = max(
            reduce(max, mapEndTime.values()),
            reduce(max, reduceEndTime.values()))
    else:
        endTime = reduce(max, mapEndTime.values())
    return endTime

def report_job_analysis(cluster, job_list, output_file):

    task_detail_list = {}
    hs = cluster.getHistoryServer()

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

    for job in job_list:
        job_id = job.id
        job_detail = historytool.get_job_detail(
            cluster.getHistoryServer(), job_id)
        task_list = historytool.get_task_list(
            cluster.getHistoryServer(), job_id)
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
            task_detail = historytool.get_task_flow_detail(
                cluster.getHistoryServer(), job_id, task_id)
            if "m" in task_id:
                map_flow_in.append(task_detail['flow_in'])
                map_flow_out.append(task_detail['flow_out'])
                map_elapsed_time.append(task_detail['elapsedTime'])
                map_waiting_time.append(
                    task_detail['waitingTime'] / 1000000000.0)
            elif "r" in task_id:
                reduce_flow_in.append(task_detail['flow_in'])
                reduce_flow_out.append(task_detail['flow_out'])
                reduce_elapsed_time.append(task_detail['elapsedTime'])
                reduce_waiting_time.append(
                    task_detail['waitingTime'] / 1000000000.0)

        fd.write("%s,%s,%s,%s" %
                 (job_id, job_detail['name'], job_detail['mapsTotal'], job_detail['reducesTotal']))
        fd.write(",%s" % (job_detail['finishTime'] - job_detail['startTime']))
        fd.write(",%s,%s" %
                 (numpy.mean(map_elapsed_time), numpy.std(map_elapsed_time)))
        fd.write(",%s,%s" %
                 (numpy.min(map_elapsed_time), numpy.max(map_elapsed_time)))
        fd.write(",%s,%s" % (numpy.mean(map_flow_in), numpy.std(map_flow_in)))
        fd.write(",%s,%s" %
                 (numpy.mean(map_flow_out), numpy.std(map_flow_out)))
        fd.write(",%s,%s" %
                 (numpy.mean(map_waiting_time), numpy.std(map_waiting_time)))
        fd.write(",%s,%s" %
                 (numpy.mean(reduce_elapsed_time), numpy.std(reduce_elapsed_time)))
        if len(reduce_elapsed_time) != 0:
            fd.write(",%s,%s" %
                     (numpy.min(reduce_elapsed_time), numpy.max(reduce_elapsed_time)))
        else:
            fd.write(",%s,%s" % (0, 0))
        fd.write(",%s,%s" %
                 (numpy.mean(reduce_flow_in), numpy.std(reduce_flow_in)))
        fd.write(",%s,%s" %
                 (numpy.mean(reduce_flow_out), numpy.std(reduce_flow_out)))
        fd.write(",%s,%s" %
                 (numpy.mean(reduce_waiting_time), numpy.std(reduce_waiting_time)))
        fd.write("\n")
            #numpy.min(reduce_elapsed_time), numpy.max(reduce_elapsed_time), \
            #numpy.mean(reduce_waiting_time), numpy.std(reduce_waiting_time)
    fd.flush()
    fd.close()


def report_waiting_time(cluster, job_list, output_file):
    '''
       This requires modification in Hadoop of adding a counter to record wait time
    '''
    hs = cluster.getHistoryServer()

    f = open(output_file, "w")

    task_list = {}

    for job in job_list:
        job_id = job.id
        task_id_list = historytool.get_task_list(hs, job_id)
        if task_id_list is None:
            continue
        for task_id in task_id_list:
            task_detail = historytool.get_task_detail(hs, job_id, task_id)
            task_list[task_id] = task_detail

    mapWaitingTime = {}
    reduceWaitingTime = {}
    startTime = int(min([value['startTime']
                    for (key, value) in task_list.iteritems()]))
    endTime = int(max([value['finishTime']
                  for (key, value) in task_list.iteritems()]))

    for t in range(startTime, endTime):
        mapWaitingTime[t] = 0.0
        reduceWaitingTime[t] = 0.0

    for task_id in task_list.keys():
        if "waitingTime" not in task_list[task_id]:
            print "missing waiting time counter: %s" % task_id
            continue
        for t in range(int(task_list[task_id]['startTime']), int(task_list[task_id]['finishTime'])):
            if 'm' in task_id:
                mapWaitingTime[t] += task_list[task_id]['waitingTime'] / 1000000000.0 / \
                    (task_list[task_id]['finishTime']
                     - task_list[task_id]['startTime'])
            elif 'r' in task_id:
                reduceWaitingTime[t] += task_list[task_id]['waitingTime'] / 1000000000.0 / \
                    (task_list[task_id]['finishTime']
                     - task_list[task_id]['startTime'])

    f.write("time,map_waiting_time,reduce_waiting_time,total_waiting_time\n")
    for t in range(startTime, endTime):
        f.write(
            "%s,%s,%s,%s\n" % (datetime.datetime.fromtimestamp(t).strftime('%Y-%m-%d %H:%M:%S'),
                               mapWaitingTime[t], reduceWaitingTime[t], mapWaitingTime[t] + reduceWaitingTime[t]))
    f.close()

def report_task_timeline(cluster, jobs, output_file):

    hs = cluster.getHistoryServer()

    (task_job_mapping, task_job_id_mapping, map_list, reduce_list, mapStartTime, mapEndTime,
     reduceStartTime, reduceEndTime, reduceShuffleTime, reduceMergeTime) = historytool.get_time_counters(hs, jobs)

    runningMaps = {}
    shufflingReduces = {}
    mergingReduces = {}
    runningReduces = {}
    startTime = get_start_time(mapStartTime, reduceStartTime)
    endTime = get_end_time(mapEndTime, reduceEndTime)


    # print startTime, endTime
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
        timestamp = datetime.datetime.fromtimestamp(
            t).strftime('%Y-%m-%d %H:%M:%S')
        f.write("%s,%s,%s,%s,%s\n" %
                (timestamp, runningMaps[t], shufflingReduces[t], mergingReduces[t], runningReduces[t]))
    f.close()

def report_flow_timeline(cluster, jobs, output_file):

    hs = cluster.getHistoryServer()

    flow_in_table = {
        "terasort": 6.2,
        "grep": 13.2,
        "word count": 3.6,
        "nocomputation": 16.2,
        "histogram-movies": 10.3,
        "histogram-ratings": 3.4,
        "inverted-index": 3.0,
        "custommap_1": 9.3,
        "custommap_2": 5.4,
        "custommap_3": 3.6,
        "custommap_4": 2.6,
        "custommap_5": 2.0,
        "custommap_6": 1.7,
        "default": 5,
    }
    flow_out_table = {
        "terasort": 9.6,
        "grep": 0,
        "word count": 3.7,
        "nocomputation": 0,
        "histogram-movies": 0,
        "histogram-ratings": 0,
        "inverted-index": 3.4,
        "custommap_1": 0,
        "custommap_2": 0,
        "custommap_3": 0,
        "custommap_4": 0,
        "custommap_5": 0,
        "custommap_6": 0,
        "default": 0,
    }

    (task_job_mapping, task_job_id_mapping, ap_list, reduce_list, mapStartTime, mapEndTime,
     reduceStartTime, reduceEndTime, reduceShuffleTime, reduceMergeTime) = historytool.get_time_counters(hs, jobs)

    runningMaps = {}
    shufflingReduces = {}
    mergingReduces = {}
    runningReduces = {}
    mapFlowIn = {}
    mapFlowOut = {}
    reduceFlowIn = {}
    reduceFlowOut = {}
    startTime = get_start_time(mapStartTime, reduceStartTime)
    endTime = get_end_time(mapEndTime, reduceEndTime)

    # print startTime, endTime
    for t in range(startTime, endTime):
        # print t
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
                    mapFlowIn[t] += float(flow) / len(r)
            if not found:
                mapFlowIn[t] += float(flow_in_table['default']) / len(r)

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
                    reduceFlowOut[t] += float(flow) / len(r)
                    found = True
            if not found:
                reduceFlowOut[t] += float(flow_out_table['default']) / len(r)

    f = open(output_file, "w")
    f.write("time,maps,shuffle,merge,reduce,map_flow_in, reduce_flow_out\n")
    for t in range(startTime, endTime):
        #f.write("%s,%s,%s,%s,%s,%s,%s\n" % (t - startTime, runningMaps[t], shufflingReduces[t], mergingReduces[t], runningReduces[t], mapFlowIn[t], reduceFlowOut[t]))
        timestamp = datetime.datetime.fromtimestamp(
            t).strftime('%Y-%m-%d %H:%M:%S')
        f.write("%s,%s,%s,%s,%s,%s,%s\n" % (timestamp, runningMaps[t], shufflingReduces[t], mergingReduces[t], runningReduces[t], mapFlowIn[t], reduceFlowOut[t])
                )
    f.close()


def report_progress_timeline(cluster, jobs, output_file, aggregate=False):

    hs = cluster.getHistoryServer()

    (task_job_mapping, task_job_id_mapping, map_list, reduce_list, mapStartTime, mapEndTime,
     reduceStartTime, reduceEndTime, reduceShuffleTime, reduceMergeTime) = historytool.get_time_counters(hs, jobs)

    job_name_mapping = {}
    if aggregate:
        for job in jobs:
            job_detail = historytool.get_job_detail(hs, job.id)
            job_name_mapping[job.id] = job_detail['name']

    startTime = get_start_time(mapStartTime, reduceStartTime)
    endTime = get_end_time(mapEndTime, reduceEndTime)

    # initialize
    total_timeline = {}
    job_timeline = {}
    for t in range(startTime, endTime):
        total_timeline[t] = 0
    for job in jobs:
        throughput = {}
        for t in range(startTime, endTime):
            throughput[t] = 0
        job_key = job_name_mapping[job.id] if aggregate else job.id
        job_timeline[job_key] = throughput
    job_id_mapping = {}
    for job in jobs:
        job_id_mapping[job.id] = "%s_%s_%s" % (job.id, job.name, job.size)

    # value extraction
    for mapper in mapStartTime.keys():
        job_key = job_name_mapping[task_job_id_mapping[mapper]] if aggregate else task_job_id_mapping[mapper]
        task_detail = historytool.get_task_detail(hs, task_job_id_mapping[mapper], mapper)
        bytes_processed = task_detail['BYTES_READ'] if "MAP" == task_detail[
            'type'] else task_detail['BYTES_WRITTEN']
        bytes_per_second = bytes_processed / float(mapEndTime[mapper] - mapStartTime[mapper])
        for t in range(mapStartTime[mapper], mapEndTime[mapper]):
            job_timeline[job_key][t] += bytes_per_second

    for reducer in reduceStartTime.keys():
        period = reduceEndTime[reducer] - reduceStartTime[reducer]
        if period == 0:
            continue
        job_key = job_name_mapping[task_job_id_mapping[reducer]] if aggregate else task_job_id_mapping[reducer]
        task_detail = historytool.get_task_detail(hs, task_job_id_mapping[reducer], reducer)
        bytes_processed = task_detail['BYTES_READ'] if "MAP" == task_detail['type'] else task_detail['BYTES_WRITTEN']
        bytes_per_second = bytes_processed / float(reduceEndTime[reducer] - reduceStartTime[reducer])
        for t in range(reduceMergeTime[reducer], reduceEndTime[reducer]):
            job_timeline[job_key][t] += bytes_per_second

    # accumulate
    for (job_key, timeline) in job_timeline.iteritems():
        for t in range(startTime, endTime - 1):
            timeline[t + 1] += timeline[t]
    for (job_key, timeline) in job_timeline.iteritems():
        for t in range(startTime, endTime):
            total_timeline[t] += timeline[t]

    f = open(output_file, "w")
    f.write("time")
    for job_key in sorted(job_timeline.keys()):
        f.write(",%s" % job_id_mapping[job_key])
    f.write(",total_throughput")
    for job_key in sorted(job_timeline.keys()):
        f.write(",%s" % job_id_mapping[job_key])
    f.write(",total_progress")
    f.write("\n")

    for t in range(startTime, endTime):
        timestamp = datetime.datetime.fromtimestamp(
            t).strftime('%Y-%m-%d %H:%M:%S')
        f.write(timestamp)
        for job_key in sorted(job_timeline.keys()):
            throughput_timeline = job_timeline[job_key]
            throughput = throughput_timeline[t]
            f.write(",%s" % throughput)
        f.write(",%s" % total_timeline[t])
        for job_key in sorted(job_timeline.keys()):
            throughput_timeline = job_timeline[job_key]
            progress = throughput_timeline[t] / float(throughput_timeline[endTime - 1])
            f.write(",%s" % progress)
        f.write(",%s" % (total_timeline[t] / total_timeline[endTime - 1]))
        f.write("\n")

    f.close()


def main(argv):

    parser = argparse.ArgumentParser(
        description='Fetch Hadoop history from history server')

    parser.add_argument('--host', required=True,
                        help="The host adress of the history server")
    parser.add_argument('--port', default="19888", required=False,
                        help="The port of the history server. The default port number is 19888")
    parser.add_argument('-f', '--file', required=True, help="The output file")
    parser.add_argument(
        '-t', '--type', choices=["waiting_time", "task_timeline", "flow_timeline"], help="The type of report")

    args = parser.parse_args()

    if "waiting_time" == args.type:
        query_waiting_time(args.host, args.port, args.file)
    elif "task_timeline" == args.type:
        query_task_timeline(args.host, args.port, args.file)
    elif "flow_timeline" == args.type:
        query_flow_timeline(args.host, args.port, args.file)


if __name__ == "__main__":
    main(sys.argv[1:])
