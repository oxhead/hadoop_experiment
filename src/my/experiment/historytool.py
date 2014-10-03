#!/usr/bin/python

import sys
import argparse
import requests
import logging
import json
import time
import numpy

from my.hadoop.base import HistoryServer

logger = logging.getLogger(__name__)

session = requests.Session()

def get_query_url(hs):
    url = 'http://%s:%s/ws/v1/history/mapreduce' % (hs.host, hs.port)
    return url


def get_job_url(hs, job_id):
    url = '%s/jobs/%s' % (get_query_url(hs), job_id)
    return url


def get_task_url(hs, job_id, task_id):
    url = '%s/tasks/%s' % (get_job_url(hs, job_id), task_id)
    return url


def get_time_counters(hs, job_list):

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
    task_job_id_mapping = {}

    for job in job_list:
        job_id = job.id
        job_detail = get_job_detail(hs, job_id)
        task_list = get_task_list(hs, job_id)

        if job_detail is None or task_list is None:
            print 'Unable to get data for job', job_id
            continue

        job_name = job_detail['name']
        for task_id in task_list:
            task_job_mapping[task_id] = job_name
            task_job_id_mapping[task_id] = job_id
            task_counter = get_task_time_counters(hs, job_id,
                                                  task_id)

            (taskMapStartTime,
             taskMapEndTime,
             taskReduceStartTime,
             taskReduceEndTime,
             taskReduceShuffleTime,
             taskReduceMergeTime,
             ) = get_task_time_counters(hs, job_id, task_id)
            if 'm' in task_id:
                mapStartTime[task_id] = taskMapStartTime
                mapEndTime[task_id] = taskMapEndTime
                map_list.append(task_id)
            elif 'r' in task_id:
                reduceStartTime[task_id] = taskReduceStartTime
                reduceEndTime[task_id] = taskReduceEndTime
                reduceShuffleTime[task_id] = taskReduceShuffleTime
                reduceMergeTime[task_id] = taskReduceMergeTime
                reduce_list.append(task_id)
    return (
        task_job_mapping,
        task_job_id_mapping,
        map_list,
        reduce_list,
        mapStartTime,
        mapEndTime,
        reduceStartTime,
        reduceEndTime,
        reduceShuffleTime,
        reduceMergeTime,
    )


    # at attempt level
    # only second accuracy

def get_task_time_counters(hs, job_id, task_id):
    mapStartTime = None
    mapEndTime = None
    reduceStartTime = None
    reduceShuffleTime = None
    reduceMergeTime = None
    reduceEndTime = None

    attempt_list_url = get_task_url(hs, job_id, task_id) + '/attempts'
    attempt_list_json = session.get(attempt_list_url)
    for attempt in attempt_list_json.json()['taskAttempts'
                                            ]['taskAttempt']:
        attempt_url = attempt_list_url + '/' + attempt['id']
        attempt_json = attempt_json = session.get(attempt_url)
        json_content = attempt_json.json()
        attempt_id = json_content['taskAttempt']['id']
        attempt_type = json_content['taskAttempt']['type']
        attempt_state = json_content['taskAttempt']['state']
        attempt_startTime = json_content['taskAttempt']['startTime']
        attempt_endTime = json_content['taskAttempt']['finishTime']
        if attempt_type == 'MAP':
            mapStartTime = int(attempt_startTime) / 1000
            mapEndTime = int(attempt_endTime) / 1000
        elif attempt_type == 'REDUCE':
            attempt_shuffleTime = json_content['taskAttempt'
                                               ]['shuffleFinishTime']
            attempt_mergeTime = json_content['taskAttempt'
                                             ]['mergeFinishTime']
            reduceStartTime = int(attempt_startTime) / 1000
            reduceEndTime = int(attempt_endTime) / 1000
            reduceShuffleTime = int(attempt_shuffleTime) / 1000
            reduceMergeTime = int(attempt_mergeTime) / 1000
        attempt_json.close()
    attempt_list_json.close()
    return (
        mapStartTime,
        mapEndTime,
        reduceStartTime,
        reduceEndTime,
        reduceShuffleTime,
        reduceMergeTime,
    )


    # only at task level, but not task attempt level

def get_task_counters(hs, job_id, task_id):
    task_url = get_task_url(hs, job_id, task_id)
    task_json_response = session.get(task_url)
    task_json = task_json_response.json()
    task_json_response.close()
    if task_json['task']['state'] != 'SUCCEEDED':
        return

    counters = {}
    counters['type'] = task_json['task']['type']
    counters['startTime'] = task_json['task']['startTime'] / 1000.0
    counters['finishTime'] = task_json['task']['finishTime'] / 1000.0

    counters_url = '%s/counters' % task_url
    counters_json_response = session.get(counters_url)
    counters_json = counters_json_response.json()
    counters_json_response.close()

        # initialize

    counters['MAP_OUTPUT_MATERIALIZED_BYTES'] = 0
    for counter_group in counters_json['jobTaskCounters'
                                       ]['taskCounterGroup']:
        for counter in counter_group['counter']:
            if 'MAP' == counters['type']:
                if counter_group['counterGroupName'] \
                    == 'org.apache.hadoop.mapreduce.FileSystemCounter':
                    pass
                elif counter_group['counterGroupName'] \
                    == 'org.apache.hadoop.mapreduce.TaskCounter':
                    if counter['name'] \
                        == 'MAP_OUTPUT_MATERIALIZED_BYTES':
                        counters['MAP_OUTPUT_MATERIALIZED_BYTES'] = \
                            counter['value']
                elif counter_group['counterGroupName'] \
                    == 'org.apache.hadoop.mapreduce.lib.input.FileInputFormatCounter':
                    if counter['name'] == 'BYTES_READ':
                        counters['BYTES_READ'] = counter['value']
                elif counter_group['counterGroupName'] == 'my':
                    if counter['name'] == 'map_waiting_time':
                        counters['waitingTime'] = counter['value']
            elif 'REDUCE' == counters['type']:
                if counter_group['counterGroupName'] \
                    == 'org.apache.hadoop.mapreduce.FileSystemCounter':
                    pass
                elif counter_group['counterGroupName'] \
                    == 'org.apache.hadoop.mapreduce.TaskCounter':
                    if counter['name'] == 'REDUCE_SHUFFLE_BYTES':
                        counters['REDUCE_SHUFFLE_BYTES'] = \
                            counter['value']
                elif counter_group['counterGroupName'] \
                    == 'org.apache.hadoop.mapreduce.lib.output.FileOutputFormatCounter':
                    if counter['name'] == 'BYTES_WRITTEN':
                        counters['BYTES_WRITTEN'] = counter['value']
                elif counter_group['counterGroupName'] == 'my':
                    if counter['name'] == 'reduce_waiting_time':
                        counters['waitingTime'] = counter['value']

    return counters


    # time_start: sec
    # time_end: sec, e.g. time.time()

def get_job_list(hs, time_start=None, time_end=None):
    time_start = (time_start if time_start is not None else '')
    time_end = (time_end if time_end is not None else '')
    job_list_url = '%s/jobs' % get_query_url(hs)
    job_list_query_url = '%s?startedTimeBegin=%s&finishTimeEnd=%s' \
        % (job_list_url, time_start * 1000, time_end * 1000)
    jobs = []
    job_list_json = session.get(job_list_query_url)
    if job_list_json.status_code != 200:
        raise Exception('Unable to get task list for job: %s', job_id)
    jobs = job_list_json.json()['jobs']['job']
    job_list_json.close()
    job_list = []
    for job in jobs:
        if job['state'] == 'SUCCEEDED':
            job_list.append(job['id'])

    return job_list


def get_task_list(hs, job_id, type=None):
    tasks_url = '%s/tasks' % get_job_url(hs, job_id)
    tasks_json_response = session.get(tasks_url)
    if tasks_json_response.status_code != 200:
        raise Exception('Unable to get task list for job: %s' % job_id)
    tasks_json = tasks_json_response.json()
    tasks_json_response.close()

    map_task_list = []
    reduce_task_list = []
    for task in tasks_json['tasks']['task']:
        if task['state'] == 'SUCCEEDED':
            if task['type'] == 'MAP':
                map_task_list.append(task['id'])
            elif task['type'] == 'REDUCE':
                reduce_task_list.append(task['id'])
    task_list = []
    if type is None or type.lower() == 'map':
        for task in map_task_list:
            task_list.append(task)
    if type is None or type.lower() == 'reduce':
        for task in reduce_task_list:
            task_list.append(task)

    return task_list


def get_task_detail(hs, job_id, task_id):
    task_detail = get_task_counters(hs, job_id, task_id)
    return task_detail


def get_job_detail(hs, job_id):
    job_url = 'http://%s:%s/ws/v1/history/mapreduce/jobs/%s' \
        % (hs.host, hs.port, job_id)
    logger.debug(job_url)
    job_json_response = session.get(job_url)
    if job_json_response.status_code != 200:
        return None
    job_json = job_json_response.json()
    job_json_response.close()
    if job_json['job']['state'] != 'SUCCEEDED':
        return None

    return job_json
    '''
    counters = {}
    counters['name'] = job_json['job']['name']
    counters['mapsTotal'] = job_json['job']['mapsTotal']
    counters['reducesTotal'] = job_json['job']['reducesTotal']
    counters['startTime'] = job_json['job']['startTime'] / 1000.0
    counters['finishTime'] = job_json['job']['finishTime'] / 1000.0
    return counters
    '''


def get_job_elapsed_time(hs, job_id):
    job_detail = get_job_detail(hs, job_id)
    return job_detail['finishTime'] - job_detail['startTime']


def get_elapsed_time(hs, job_id_list):
    start_time = sys.maxint
    finish_time = -1
    for job_id in job_id_list:
        job_detail = get_job_detail(self, job_id)
        start_time = (job_detail['startTime'] if job_detail['startTime'
                                                            ] < start_time else start_time)
        finish_time = (job_detail['finishTime'
                                  ] if job_detail['finishTime']
                       > finish_time else finish_time)
    return (start_time, finish_time)


def get_task_flow_detail(hs, job_id, task_id):
    task_detail = get_task_detail(hs, job_id, task_id)
    elapsed_task_time = task_detail['finishTime'] \
        - task_detail['startTime']
    task_detail['elapsedTime'] = elapsed_task_time

    if task_detail['type'] == 'MAP':
        task_detail['input_size'] = int(task_detail['BYTES_READ'])
        task_detail['output_size'] = \
            int(task_detail['MAP_OUTPUT_MATERIALIZED_BYTES'])
        task_detail['flow_in'] = task_detail['input_size'] / (1024.0
                                                              * 1024.0) / task_detail['elapsedTime']
        task_detail['flow_out'] = task_detail['output_size'] / (1024.0
                                                                * 1024.0) / task_detail['elapsedTime']
    elif task_detail['type'] == 'REDUCE':
        task_detail['input_size'] = \
            int(task_detail['REDUCE_SHUFFLE_BYTES'])
        task_detail['output_size'] = int(task_detail['BYTES_WRITTEN'])
        task_detail['flow_in'] = task_detail['input_size'] / (1024.0
                                                              * 1024.0) / task_detail['elapsedTime']
        task_detail['flow_out'] = task_detail['output_size'] / (1024.0
                                                                * 1024.0) / task_detail['elapsedTime']
    return task_detail

'''
-----------------------------
'''

def get_json_job_list(hs, time_start=None, time_end=None):
    time_start = (time_start if time_start is not None else '')
    time_end = (time_end if time_end is not None else '')
    job_list_url = '%s/jobs' % get_query_url(hs)
    job_list_query_url = '%s?startedTimeBegin=%s&finishTimeEnd=%s' \
        % (job_list_url, time_start * 1000, time_end * 1000)
    job_list_json = session.get(job_list_query_url)
    if job_list_json.status_code != 200:
        raise Exception('Unable to get task list for job: %s', job_id)
    return job_list_json.json()['jobs']['job']

def get_json_job_detail(hs, job_id):
    job_url = get_job_url(hs, job_id)
    job_json = session.get(job_url)
    if job_json.status_code != 200:
        return None

    return job_json.json()['job']

def get_json_task_list(hs, job_id, type=None):
    tasks_url = '%s/tasks' % get_job_url(hs, job_id)
    tasks_json_response = session.get(tasks_url)
    if tasks_json_response.status_code != 200:
        raise Exception('Unable to get task list for job: %s' % job_id)
    return tasks_json_response.json()['tasks']['task']
    
def get_json_task_detail(hs, job_id, task_id):
    task_url = get_task_url(hs, job_id, task_id)
    task_json = session.get(task_url)
    if task_json.status_code != 200:
        return None
    return task_json.json()['task']

def get_json_task_counters(hs, job_id, task_id):
    task_url = get_task_url(hs, job_id, task_id)
    counters_url = '%s/counters' % task_url
    counters_json_response = session.get(counters_url)
    return counters_json_response.json()['jobTaskCounters']['taskCounterGroup']

def get_json_attempt_list(hs, job_id, task_id):
    task_url = get_task_url(hs, job_id, task_id)
    attempts_url = '%s/attempts' % task_url
    attempts_json = session.get(attempts_url)
    return attempts_json.json()['taskAttempts']['taskAttempt']

def get_json_attempt_counters(hs, job_id, task_id, attempt_id):
    task_url = get_task_url(hs, job_id, task_id)
    counters_url = '%s/attempts/%s/counters' % (task_url, attempt_id)
    counters_json = session.get(counters_url)
    return counters_json.json()['jobTaskAttemptCounters']['taskAttemptCounterGroup']

def dump(hs, output_path, time_start=None, time_end=None):
  
    data = {'jobs': {}} 
    jobs = json.loads('{}')
    jobs = get_json_job_list(hs, time_start, time_end)
    for job in jobs:
        job_id = job['id']
        job_detail = get_json_job_detail(hs, job_id) 
        data['jobs'][job_id] = job_detail
        
        data['jobs'][job_id]['tasks'] = {}
        tasks = get_json_task_list(hs, job_id)
        for task in tasks:
            task_id = task.pop('id')
            data['jobs'][job_id]['tasks'][task_id] = {}
            data['jobs'][job_id]['tasks'][task_id].update(task)

            ''' task counters'''
            task_counters = get_json_task_counters(hs, job_id, task_id)
            aggregate_counters = {}
            for task_counter_group in task_counters:
                counter_group_name = task_counter_group['counterGroupName']
                counters = {}
                for counter in task_counter_group['counter']:
                    counters[counter['name']] = counter['value']
                aggregate_counters[counter_group_name] = counters
            data['jobs'][job_id]['tasks'][task_id]['counters'] = aggregate_counters

            ''' attempt counters '''
            data['jobs'][job_id]['tasks'][task_id]['taskAttempts'] = {}
            attempts = get_json_attempt_list(hs, job_id, task_id)
            for attempt in attempts:
                attempt_id = attempt.pop('id')
                data['jobs'][job_id]['tasks'][task_id]['taskAttempts'][attempt_id] = {}
                data['jobs'][job_id]['tasks'][task_id]['taskAttempts'][attempt_id].update(attempt)

                attempt_counters = get_json_attempt_counters(hs, job_id, task_id, attempt_id)
                aggregate_attempt_counters = {}
                for attempt_counter_group in attempt_counters:
                    attempt_counter_group_name = attempt_counter_group['counterGroupName']
                    attempt_counters = {}
                    for attempt_counter in attempt_counter_group['counter']:
                        attempt_counters[attempt_counter['name']] = attempt_counter['value']
                    aggregate_attempt_counters[attempt_counter_group_name] = attempt_counters
                data['jobs'][job_id]['tasks'][task_id]['taskAttempts'][attempt_id]['counters'] = aggregate_attempt_counters
 
    with open(output_path, "w") as f:
        json.dump(data, f, sort_keys=True)


def main(argv):
    parser = argparse.ArgumentParser(description='Job ID')
    parser.add_argument('--host', required=True,
                        help='The host adress of the history server')
    parser.add_argument('--port', default='19888', required=False,
                        help='The port of the history server. The default port number is 19888'
                        )

    args = parser.parse_args()
    hs = HistoryServer(args.host, args.port)
    dump(hs)
    sys.exit(0)


if __name__ == '__main__':
    main(sys.argv[1:])
