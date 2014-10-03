from peewee import *
import sys
import json
import datetime
import os.path
import logging

from my.datastore.base import *

logger = logging.getLogger(__name__)

def importJsonToDatabase(data):

    with transaction():
        run = HadoopRun.create(id="test", description="balabala", is_completed=True, completion_time=123)
        for (job_id, job_value) in data['jobs'].iteritems():
            job = Job.create(
                run=run, \
                id=job_id, \
                name=job_value['name'], \
                state=job_value['state'], \
                user=job_value['user'], \
                queue=job_value['queue'], \
                submit_time=job_value['submitTime'], \
                start_time=job_value['startTime'], \
                finish_time=job_value['finishTime'], \
                avg_map_time=job_value['avgMapTime'], \
                avg_shuffle_time=job_value['avgShuffleTime'], \
                avg_merge_time=job_value['avgMergeTime'], \
                avg_reduce_time=job_value['avgReduceTime'], \
                maps_total=job_value['mapsTotal'], \
                reduces_total=job_value['reducesTotal'], \
                maps_completed=job_value['mapsCompleted'], \
                reduces_completed=job_value['reducesCompleted'], \
                successful_map_attempts=job_value['successfulMapAttempts'], \
                successful_reduce_attempts=job_value['successfulReduceAttempts'], \
                failed_map_attempts=job_value['failedMapAttempts'], \
                failed_reduce_attempts=job_value['failedReduceAttempts'], \
                killed_map_attempts=job_value['killedMapAttempts'], \
                killed_reduce_attempts=job_value['killedReduceAttempts']
            )

            for (task_id, task_value) in job_value['tasks'].iteritems():
                task = Task.create(
                   job=job, \
                   id=task_id, \
                   type=task_value['type'], \
                   state=task_value['state'], \
                   start_time=task_value['startTime'], \
                   finish_time=task_value['finishTime'], \
                   elapsed_time=task_value['elapsedTime'], \
                   progress=task_value['progress']
                )

                ''' insert task counters'''
                for (counter_group_name, counter_groups) in task_value['counters'].iteritems():
                    if not CounterGroup.select().where(CounterGroup.name == counter_group_name).exists():
                        CounterGroup.create(name=counter_group_name) 
                    counter_group = CounterGroup.get(CounterGroup.name == counter_group_name)

                    for (counter_name, value) in counter_groups.iteritems():
                        if not Counter.select().where(Counter.name == counter_name, Counter.group == counter_group).exists():
                            Counter.create(name=counter_name, group=counter_group)
                        counter = Counter.get(Counter.name == counter_name, Counter.group == counter_group)
                        TaskCounterRecord.create(task=task, counter=counter, value=value)

                for (attempt_id, attempt_value) in task_value['taskAttempts'].iteritems():
                    attempt = TaskAttempt.create(
                        task=task, \
                        id=attempt_id, \
                        state=attempt_value['state'], \
                        type=attempt_value['type'], \
                        start_time=attempt_value['startTime'], \
                        finish_time=attempt_value['finishTime'], \
                        elapsed_time=attempt_value['elapsedTime'], \
                        assigned_container_id=attempt_value['assignedContainerId'], \
                        node_http_address=attempt_value['nodeHttpAddress'], \
                        rack=attempt_value['rack'] 
                    )

                    for (attempt_counter_group_name, attempt_counter_groups) in attempt_value['counters'].iteritems():
                        if not CounterGroup.select().where(CounterGroup.name == attempt_counter_group_name).exists():
                            CounterGroup.create(name=attempt_counter_group_name)
                        attempt_counter_group = CounterGroup.get(CounterGroup.name == attempt_counter_group_name)
                        for (attempt_counter_name, attempt_value) in attempt_counter_groups.iteritems():
                            if not Counter.select().where(Counter.name == attempt_counter_name, Counter.group.contains(attempt_counter_group_name)).exists():
                                Counter.create(name=attempt_counter_name, group=attempt_counter_group)
                            attempt_counter = Counter.get(Counter.name == attempt_counter_name, Counter.group.contains(attempt_counter_group_name))
                            TaskAttemptCounterRecord.create(task_attempt=attempt, counter=attempt_counter, value=attempt_value)


def main(argv):
    #clear("history.db")
    init("history.db")
    data = loadJson("json.json")
    importJsonToDatabase(data)
    sys.exit(0)


if __name__ == '__main__':
    main(sys.argv[1:])
