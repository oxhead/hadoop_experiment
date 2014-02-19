#!/usr/bin/python

import sys
import os
import argparse
from time import sleep
import tempfile
import datetime
import time
from command import *
import generate_configuration
import generate_topology
from topology import *
from command import Command
import myinfo
import mylog

supported_jobs = [
	'wordcount',
	'grep',
	'terasort',
	'custommap',
]

def lookup_size_name(job_size):
	if job_size < 1024:
		return "%sMB" % job_size
	elif job_size % 1024 == 0:
		return "%sGB" % (job_size / 1024)
	else:
		return None

def convert_unit(size):
        if "MB" in size:
                return int(size.replace("MB", ""))
        elif "GB" in size:
                return int(size.replace("GB", "")) * 1024

def get_job_setting(job=None, map_size=1024, job_size=1024, num_reducers=1, prefix=None, log_dir="log"):
	now = datetime.datetime.now()
	setting = {}
	setting['hadoop_dir'] = "~/hadoop"
	setting['output_dir'] = "/output"
	setting['log_dir'] = log_dir
	setting['dataset'] = "/dataset/%s_%s" % ("terasort" if (job is None or ("terasort" in job)) else "wikipedia", lookup_size_name(job_size))	
	setting['job_id'] = "%s_%s_%s_%s" % (prefix, job, lookup_size_name(job_size), now.strftime("%Y-%m-%d_%H-%M-%S") ) if prefix is not None else "%s_%s_%s" % (job, lookup_size_name(job_size), now.strftime("%Y-%m-%d_%H-%M-%S") )
	setting['job_output'] = "%s/%s" % (setting['output_dir'], setting['job_id'])
	setting['job_log'] = "%s/%s.log" % (setting['log_dir'], setting['job_id'])
	setting['job_returncode'] = "%s/%s.returncode" % (setting['log_dir'], setting['job_id'])
	setting['map_size'] = map_size
	return setting

def generate_command(job, setting):
	cmd = None

        if "wordcount" == job:
                cmd = "%s/bin/hadoop jar %s/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar wordcount -Dmapreduce.job.reduces=1 -Dmapreduce.map.memory.mb=%s %s %s > %s 2>&1 ; echo $? > %s" % (setting['hadoop_dir'], setting['hadoop_dir'], setting['map_size'], setting['dataset'], setting['job_output'], setting['job_log'], setting['job_returncode'])
        elif "grep" == job:
                pattern = "hadoop"
                cmd = "%s/bin/hadoop jar %s/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar grep -Dmapreduce.job.reduces=1 -Dmapreduce.map.memory.mb=%s %s %s \"%s\" > %s 2>&1 ; echo $? > %s" % (setting['hadoop_dir'], setting['hadoop_dir'], setting['map_size'], setting['dataset'], setting['job_output'], pattern, setting['job_log'], setting['job_returncode'])
        elif "terasort" == job:
                cmd = "%s/bin/hadoop jar %s/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar terasort -Dmapreduce.job.reduces=1 -Dmapreduce.map.memory.mb=%s %s %s > %s 2>&1 ; echo $? > %s" % (setting['hadoop_dir'], setting['hadoop_dir'], setting['map_size'], setting['dataset'], setting['job_output'], setting['job_log'], setting['job_returncode'])
        elif "nocomputation" == job:
                cmd = "%s/bin/hadoop jar %s/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar custommap -Dmapreduce.job.reduces=1 -Dmapreduce.map.memory.mb=%s %s %s 0 1 1 1024 > %s 2>&1 ; echo $? > %s" % (setting['hadoop_dir'], setting['hadoop_dir'], setting['map_size'], setting['dataset'], setting['job_output'], setting['job_log'], setting['job_returncode'])

	return cmd

def submit_custom(command, task_log):
	cmd = Command(command).run()
	job_ids = mylog.lookup_job_ids(task_log)
	return job_ids

def submit_async(job, map_size=2048, job_size=64, num_reducers=1, log_dir="log", prefix=None):
	setting = get_job_setting(job=job, job_size=job_size, map_size=map_size, num_reducers=num_reducers, prefix=prefix, log_dir=log_dir)
        cmd = generate_command(job, setting)
        print cmd

	os.system(" (%s) &" % cmd)

	return setting
		
def submit(job, map_size=2048, job_size=64, num_reducers=1, log_dir="log", prefix=None):
	setting = get_job_setting(job=job, job_size=job_size, map_size=map_size, num_reducers=num_reducers, prefix=prefix, log_dir=log_dir)
	cmd = generate_command(job, setting)
	print cmd

	Command(cmd).run()

	return setting

	# job_ids = mylog.lookup_job_ids(setting['job_log'])
	# return job_ids

def wait_completion(returncode_list):
	while True:
        	all_pass = True
                for job in returncode_list:
                	if not os.path.exists(job):
                        	all_pass = False
                if all_pass:
                	break
                sleep(1)
def clean_job(setting):
	cmd = "%s/bin/hadoop dfs -rm -r %s" % (setting['hadoop_dir'], setting['job_output'])
	Command(cmd).run()

def main(argv):
	parser = argparse.ArgumentParser(description='Configuration generator')
	parser.add_argument("-m", "--map", type=int, required=False, default=1024, help="The memory usage of each map with the unit MB")
	parser.add_argument("-r", "--reducer", type=int, required=False, default=1, help="The number of reduce slots")
	parser.add_argument("-s", "--size", type=int, required=True, choices=[64, 1024], help="The data size")
	parser.add_argument("-j", "--job", required=True, help="The job name to measure flow demand")

	args = parser.parse_args()
	job_ids = submit(args.job, map_size=args.map, job_size=args.size, num_reducers=args.reducer)
	for job_id in job_ids:
		myjob.print_detail(job_id)

if __name__ == "__main__":
        main(sys.argv[1:])
