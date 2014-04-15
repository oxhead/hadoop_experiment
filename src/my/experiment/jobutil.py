#!/usr/bin/python

import threading
import sys
import os
import argparse
from time import sleep
import tempfile
import datetime
import time
import logging

from my.util import command
from my.experiment import helper

logger = logging.getLogger(__name__)

def generate_command(job):
    cmd = None
    hadoop_dir = helper.get_hadoop_dir()

    if "wordcount" == job.name:
        cmd = "%s/bin/hadoop jar %s/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar wordcount -Dmapreduce.job.reduces=%s -Dmapreduce.map.memory.mb=%s %s %s > %s 2>&1; echo $? > %s" % (
            hadoop_dir, hadoop_dir, job.num_reducers, job.map_size, job.input_dir, job.output_dir, job.log_path, job.returncode_path)
    elif "invertedindex" == job.name:
        cmd = "%s/bin/hadoop jar %s/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar invertedindex -Dmapreduce.job.reduces=%s -Dmapreduce.map.memory.mb=%s %s %s > %s 2>&1; echo $? > %s" % (
            hadoop_dir, hadoop_dir, job.num_reducers, job.map_size, job.input_dir, job.output_dir, job.log_path, job.returncode_path)
    elif "termvector" == job.name:
        cmd = "%s/bin/hadoop jar %s/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar termvector -Dmapreduce.job.reduces=%s -Dmapreduce.map.memory.mb=%s %s %s > %s 2>&1; echo $? > %s" % (
            hadoop_dir, hadoop_dir, job.num_reducers, job.map_size, job.input_dir, job.output_dir, job.log_path, job.returncode_path)
    elif "histogrammovies" == job.name:
        cmd = "%s/bin/hadoop jar %s/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar histogrammovies -Dmapreduce.job.reduces=%s -Dmapreduce.map.memory.mb=%s %s %s > %s 2>&1; echo $? > %s" % (
            hadoop_dir, hadoop_dir, job.num_reducers, job.map_size, job.input_dir, job.output_dir, job.log_path, job.returncode_path)
    elif "histogramratings" == job.name:
        cmd = "%s/bin/hadoop jar %s/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar histogramratings -Dmapreduce.job.reduces=%s -Dmapreduce.map.memory.mb=%s %s %s > %s 2>&1; echo $? > %s" % (
            hadoop_dir, hadoop_dir, job.num_reducers, job.map_size, job.input_dir, job.output_dir, job.log_path, job.returncode_path)
    elif "grep" == job.name:
        pattern = "hadoop.*"
        cmd = "%s/bin/hadoop jar %s/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar grep -Dmapreduce.job.reduces=%s -Dmapreduce.map.memory.mb=%s %s %s \"%s\" > %s 2>&1; echo $? > %s" % (
            hadoop_dir, hadoop_dir, job.num_reducers, job.map_size, job.input_dir, job.output_dir, pattern, job.log_path, job.returncode_path)
    elif "grep2" == job.name:
        pattern = ".*hadoop.*"
        cmd = "%s/bin/hadoop jar %s/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar grep -Dmapreduce.job.reduces=%s -Dmapreduce.map.memory.mb=%s %s %s \"%s\" > %s 2>&1; echo $? > %s" % (
            hadoop_dir, hadoop_dir, job.num_reducers, job.map_size, job.input_dir, job.output_dir, pattern, job.log_path, job.returncode_path)
    elif "terasort" == job.name:
        cmd = "%s/bin/hadoop jar %s/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar terasort -Dmapreduce.job.reduces=%s -Dmapreduce.map.memory.mb=%s %s %s > %s 2>&1; echo $? > %s" % (
            hadoop_dir, hadoop_dir, job.num_reducers, job.map_size, job.input_dir, job.output_dir, job.log_path, job.returncode_path)
    elif "nocomputation" == job.name:
        cmd = "%s/bin/hadoop jar %s/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar nocomputation -Dmapreduce.job.reduces=%s -Dmapreduce.map.memory.mb=%s %s %s > %s 2>&1; echo $? > %s" % (
            hadoop_dir, hadoop_dir, job.num_reducers, job.map_size, job.input_dir, job.output_dir, job.log_path, job.returncode_path)
    elif "custommap" in job.name:
        timeout = 1
        params = job.params
        if len(job.name) > 9:
            timeout = int(job.name[9:])
            if params is None:
                params = {'timeout': timeout, 'num_cpu_workers': timeout,
                          'num_vm_workers': timeout, 'vm_bytes': str(1024 * 1024 * timeout)}

        cmd = "%s/bin/hadoop jar %s/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar custommap -Dmapreduce.job.reduces=%s -Dmapreduce.map.memory.mb=%s %s %s %s %s %s %s > %s 2>&1; echo $? > %s" % (
            hadoop_dir, hadoop_dir, job.num_reducers, job.map_size, job.input_dir, job.output_dir, params['timeout'], params['num_cpu_workers'], params['num_vm_workers'], params['vm_bytes'], job.log_path, job.returncode_path)

    elif "classification" == job.name:
        num_mapper = job.size / 64
        num_reducer = num_mapper / 8 if num_mapper / 8 > 0 else 1
        cmd = "%s/bin/hadoop jar %s/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar classification -m %s -r %s %s %s >%s 2>&1; echo $? > %s" % (
            hadoop_dir, hadoop_dir, num_mapper, num_reducer, job.input_dir, job.output_dir, job.log_path, job.returncode_path)
    return cmd

def lookup_job_status(job_log):
    retcode = command.execute("grep 'completed successfully' %s " % job_log)
    return True if retcode == 0 else False

def lookup_job_ids(job_log):
    cmd = command.Command("grep 'completed successfully\|failed with state' %s | tr -s ' ' | cut -d ' ' -f6" % job_log)
    cmd.run()
    return cmd.output.strip().split("\n")

def complete_job_info(job):
    job_log = job.log_path
    status = lookup_job_status(job_log)
    job_id = lookup_job_ids(job_log)[0] if status else None
    logger.debug("%s -> status: %s, job_id: %s", job_log, status, job_id)
    # TODO support multipe ids?
    job.setId(job_id)
    job.setStatus(status)
    if not status:
        logger.info("Failed job: %s", job)


def print_job_detail(job):
    logger.info("Job: %s (size=%s, dataset=%s, log=%s)" % (job.name, job.size, job.input_dir, job.log_path))

def add_timestamp(job):
    timestamp = helper.get_timestamp()
    job.output_dir = "%s_%s" % (job.output_dir, timestamp)
    job.log_path = job.log_path.replace(".log", "_%s.log" % timestamp)
    job.returncode_path = job.returncode_path.replace(".returncode", "_%s.returncode" % timestamp)

def submit_async(job):
    add_timestamp(job)
    cmd = generate_command(job)
    if cmd is None:
        raise Exception("Job command failed to generate: %s" % job.name)
    print_job_detail(job)
    command.execute_async(cmd)


def submit(job):
    add_timestamp(job)
    cmd = generate_command(job)
    if cmd is None:
        raise Exception("Job command failed to generate: %s" % job.name)
    print_job_detail(job)
    command.execute(cmd)

def wait_completion(job_list):
    for job in job_list:
        logger.debug(job.returncode_path)
    check_times = 0
    while True:
        count = 0
        all_pass = True
        for job in job_list:
            if not os.path.exists(job.returncode_path):
                logger.debug("Missing: %s", job.returncode_path)
                all_pass = False
            else:
                count = count + 1
        icon = "*" if check_times % 2 == 0 else "+"
        sys.stdout.write("\r%s Job progress -> submitted=%s, completed=%s" % (icon, len(job_list), count))
        sys.stdout.flush()

        if all_pass:
            break
        sleep(1)
        check_times = check_times + 1
    sys.stdout.write("\r\n")


def clean_job(job):
    hadoop_dir = helper.get_hadoop_dir()
    cmd = "%s/bin/hadoop dfs -rm -r %s" % (hadoop_dir, job.output_dir)
    command.execute(cmd)
