from peewee import *
import sys
import json
import datetime
import os.path
import logging

logger = logging.getLogger(__name__)

database_proxy = Proxy() 

def init(database_path):
    database_proxy.initialize(SqliteDatabase(database_path))
    if not os.path.exists(database_path):
        create_table()

def clear(database_path):
    database_proxy.initialize(SqliteDatabase(database_path))
    if os.path.exists(database_path):
        delete_table()

def transaction():
    return database_proxy.transaction()

def create_table():
    logger.info("creating database table")
    HadoopRun.create_table()
    Job.create_table()
    Task.create_table()
    TaskAttempt.create_table()
    CounterGroup.create_table()
    Counter.create_table()
    TaskCounterRecord.create_table()
    TaskAttemptCounterRecord.create_table()

def delete_table():
    logger.info("deleting database table")
    HadoopRun.drop_table()
    Job.drop_table()
    Task.drop_table()
    TaskAttempt.drop_table()
    CounterGroup.drop_table()
    Counter.drop_table()
    TaskCounterRecord.drop_table()
    TaskAttemptCounterRecord.drop_table()

class BaseModel(Model):
    class Meta:
        database = database_proxy

class HadoopRun(BaseModel):
    id = CharField(primary_key=True)
    description = TextField()
    is_completed = BooleanField()
    created_date = DateTimeField(default=datetime.datetime.now)
    completion_time = IntegerField()

class Job(BaseModel):
    run = ForeignKeyField(HadoopRun, related_name='run')
    id = CharField(primary_key=True)
    name = CharField()
    state = CharField()
    user = CharField()
    queue = CharField()
    submit_time = DateTimeField()
    start_time = DateTimeField()
    finish_time = DateTimeField()
    avg_map_time = IntegerField()
    avg_shuffle_time = IntegerField()
    avg_merge_time = IntegerField()
    avg_reduce_time = IntegerField()
    maps_total = IntegerField()
    reduces_total = IntegerField()
    maps_completed = IntegerField()
    reduces_completed = IntegerField()
    successful_map_attempts = IntegerField()
    successful_reduce_attempts = IntegerField()
    failed_map_attempts = IntegerField()
    failed_reduce_attempts = IntegerField()
    killed_map_attempts = IntegerField()
    killed_reduce_attempts = IntegerField()

class Task(BaseModel):
    job = ForeignKeyField(Job, related_name='job')
    id = CharField(primary_key=True)
    type = CharField()
    state = CharField()
    start_time = DateTimeField()
    finish_time = DateTimeField()
    elapsed_time = IntegerField()
    progress = DoubleField()

class TaskAttempt(BaseModel):
    task = ForeignKeyField(Task, related_name='task')
    id = CharField(primary_key=True)
    state = DateTimeField()
    type = CharField()
    start_time = DateTimeField()
    finish_time = DateTimeField()
    elapsed_time = IntegerField()
    assigned_container_id = CharField()
    node_http_address = CharField()
    rack = CharField()

class CounterGroup(BaseModel):
    name = CharField(primary_key=True)

class Counter(BaseModel):
    group = ForeignKeyField(CounterGroup, related_name='group')
    name = CharField(primary_key=True)

class TaskCounterRecord(BaseModel):
    task = ForeignKeyField(Task, related_name='record_task')
    counter = ForeignKeyField(Counter, related_name='counter_task')
    value = BigIntegerField()

class TaskAttemptCounterRecord(BaseModel):
    task_attempt = ForeignKeyField(TaskAttempt, related_name='record_attempt')
    counter = ForeignKeyField(Counter, related_name='counter_attempt')
    value = BigIntegerField()

def main(argv):
    sys.exit(0)

if __name__ == '__main__':
    main(sys.argv[1:])
