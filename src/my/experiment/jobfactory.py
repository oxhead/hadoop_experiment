#!/usr/bin/python

import sys
import os
from time import sleep
import datetime
import time
import random
import numpy

from my.experiment.base import *
from my.experiment import helper


def create_job(job_name, job_size, log_dir="log", map_size=1024, num_reducers=None, identifier=None):
    real_size = helper.convert_unit(job_size)
    input_dir = helper.lookup_dataset(job_name, real_size)
    output_dir = helper.get_output_dir(job_name, real_size, identifier)
    num_reducers = num_reducers if num_reducers is not None else 1 if real_size < 1024 else real_size / \
        1024
    log_path = helper.get_log_path(job_name, job_size, identifier)
    returncode_path = helper.get_returncode_path(log_path)
    job = Job(job_name, job_size, input_dir, output_dir, log_path,
              returncode_path, map_size=map_size, num_reducers=num_reducers)
    return job


'''
Create random jobs from job list and job size list
'''
def create_jobs(job_list=[], job_size_list=[], num_jobs=10, map_size=1024, log_dir="log", period=10):
    """
    job_size_list : list[1GB]
    """
    job_submit_timeline = {}
    # expected time period is 30 seconds
    rate = 1 / float(period)
    time_count = 0
    for i in range(1, num_jobs + 1):
        job_name = job_list[random.randint(0, len(job_list) - 1)]
        job_size = job_size_list[random.randint(0, len(job_size_list) - 1)]

        job = create_job(job_name, job_size,
                         log_dir=log_dir, map_size=map_size, identifier=i)

        if time_count not in job_submit_timeline:
            job_submit_timeline[time_count] = []
        job_submit_timeline[time_count].append(job)

        nextTime = int(random.expovariate(rate))
        time_count += nextTime

    return job_submit_timeline


def create_fixed_jobs(job_list=[], job_size_list=[], submit_times=10, log_dir="log", map_size=1024, period=30):
    job_submit_timeline = {}
    time_count = 0
    for job_name in job_list:
        for i in range(1, submit_times + 1):
            job_size = job_size_list[random.randint(0, len(job_size_list) - 1)]
            job = create_job(job_name, job_size,
                             log_dir=log_dir, map_size=map_size, identifier=i)
            if time_count not in job_submit_timeline:
                job_submit_timeline[time_count] = []
            job_submit_timeline[time_count].append(job)
        time_count += submit_times * period
    return job_submit_timeline

def create_ab_jobs(job_list=[], job_size="1GB", submit_times=10, submit_ratio=3, log_dir="log", map_size=1024):
    job_submit_timeline = {}
    time_count = 0
    for i in xrange(1, submit_times * submit_ratio + 1):
        job_name = job_list[0]
        job = create_job(job_name, job_size, log_dir=log_dir, map_size=map_size, identifier=i)
        if time_count not in job_submit_timeline:
            job_submit_timeline[time_count] = []
        job_submit_timeline[time_count].append(job)
    for i in xrange(1, submit_times + 1):
        job_name = job_list[1]
        job = create_job(job_name, job_size, log_dir=log_dir, map_size=map_size, identifier=i)
        if time_count not in job_submit_timeline:
            job_submit_timeline[time_count] = []
        job_submit_timeline[time_count].append(job)
    return job_submit_timeline

def create_all_pair_jobs(job_list=[], job_size_list=[], submit_times=10, log_dir="log", map_size=1024):
    job_submit_timeline = {}
    time_count = 0
    all_pair_list = [(x, y) for x in job_list for y in job_size_list]
    for (job_name, job_size) in all_pair_list:
        for i in range(1, submit_times + 1):
            job = create_job(job_name, job_size,
                             log_dir=log_dir, map_size=map_size, identifier=i)
            if time_count not in job_submit_timeline:
                job_submit_timeline[time_count] = []
            job_submit_timeline[time_count].append(job)
    return job_submit_timeline


def create_interleave_jobs(job_list=[], job_size_list=[], submit_times=10):
    job_submit_timeline = {}
    time_count = 0
    for i in xrange(submit_times):
            for job in job_list:
                job_size = job_size_list[
                    random.randint(0, len(job_size_list) - 1)]
                real_size = myjob.convert_unit(job_size)
                num_reducers = 1 if real_size < 1024 else real_size / 1024
                job_parameter = {
                    'job': job,
                    'job_size': job_size,
                    'num_reducers': num_reducers,
                }
                if time_count not in job_submit_timeline:
                    job_submit_timeline[time_count] = []
                    job_submit_timeline[time_count].append(job_parameter)
                time_count += 30
    return job_submit_timeline


def main(argv):
    parser = argparse.ArgumentParser(description='Configuration generator')
    parser.add_argument("-d", "--directory", required=True,
                        help="The output directory")
    parser.add_argument("-m", "--model", default="decoupled",
                        choices=["decoupled", "reference"])
    parser.add_argument("-s", "--scheduler", default="Color",
                        choices=["Flow", "Color", "Fifo"])
    args = parser.parse_args()
    run(args.directory, args.model, args.scheduler)

if __name__ == "__main__":
        main(sys.argv[1:])
