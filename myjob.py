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
from topology import *
from command import Command
import myinfo
import mylog

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

def lookup_dataset(job, job_size):
	dataset = ""
	if job == "terasort":
		dataset = "terasort"
	elif job == "classification":
		dataset = "kmeans"
	else:
		dataset = "wikipedia"
	return "/dataset/%s_%s" % (dataset, lookup_size_name(job_size))

def get_job_setting(job=None, job_params=None, map_size=1024, job_size=1024, num_reducers=1, prefix=None, log_dir="log"):
	now = datetime.datetime.now()
	prefix = "default" if prefix is None else prefix
	setting = {}
	setting['hadoop_dir'] = "~/hadoop"
	setting['output_dir'] = "/output"
	setting['log_dir'] = log_dir
	setting['dataset'] = lookup_dataset(job, job_size)
	setting['job'] = job
	setting['job_id'] = "%s_%s_%s_%s" % (prefix, job, lookup_size_name(job_size), now.strftime("%Y%m%d%H%M%S") )
	setting['job_size'] = job_size
	setting['job_params'] = job_params
	setting['job_output'] = "%s/%s" % (setting['output_dir'], setting['job_id'])
	setting['job_log'] = "%s/%s.log" % (setting['log_dir'], setting['job_id'])
	setting['job_returncode'] = "%s/%s.returncode" % (setting['log_dir'], setting['job_id'])
	setting['map_size'] = map_size
	setting['num_reducers'] = num_reducers
	return setting

def generate_command(setting):
	cmd = None

        if "wordcount" == setting['job']:
                cmd = "%s/bin/hadoop jar %s/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar wordcount -Dmapreduce.job.reduces=%s -Dmapreduce.map.memory.mb=%s %s %s > %s 2>&1 ; echo $? > %s" % (setting['hadoop_dir'], setting['hadoop_dir'], setting['num_reducers'], setting['map_size'], setting['dataset'], setting['job_output'], setting['job_log'], setting['job_returncode'])
        elif "grep" == setting['job']:
                pattern = "hadoop.*"
                cmd = "%s/bin/hadoop jar %s/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar grep -Dmapreduce.job.reduces=%s -Dmapreduce.map.memory.mb=%s %s %s \"%s\" > %s 2>&1 ; echo $? > %s" % (setting['hadoop_dir'], setting['hadoop_dir'], setting['num_reducers'], setting['map_size'], setting['dataset'], setting['job_output'], pattern, setting['job_log'], setting['job_returncode'])
        elif "terasort" == setting['job']:
                cmd = "%s/bin/hadoop jar %s/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar terasort -Dmapreduce.job.reduces=%s -Dmapreduce.map.memory.mb=%s %s %s > %s 2>&1 ; echo $? > %s" % (setting['hadoop_dir'], setting['hadoop_dir'], setting['num_reducers'], setting['map_size'], setting['dataset'], setting['job_output'], setting['job_log'], setting['job_returncode'])
        elif "nocomputation" == setting['job']:
                cmd = "%s/bin/hadoop jar %s/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar custommap -Dmapreduce.job.reduces=%s -Dmapreduce.map.memory.mb=%s %s %s 0 1 1 1024 > %s 2>&1 ; echo $? > %s" % (setting['hadoop_dir'], setting['hadoop_dir'], setting['num_reducers'], setting['map_size'], setting['dataset'], setting['job_output'], setting['job_log'], setting['job_returncode'])
	elif "custommap" == setting['job']:
		params = setting['job_params']
		
                cmd = "%s/bin/hadoop jar %s/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar custommap -Dmapreduce.job.reduces=%s -Dmapreduce.map.memory.mb=%s %s %s %s %s %s %s > %s 2>&1 ; echo $? > %s" % (setting['hadoop_dir'], setting['hadoop_dir'], setting['num_reducers'], setting['map_size'], setting['dataset'], setting['job_output'], params['timeout'], params['num_cpu_workers'], params['num_vm_workers'], params['vm_bytes'], setting['job_log'], setting['job_returncode'])
	elif "classification" == setting['job']:
                #cmd = "%s/bin/hadoop jar %s/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar classification -Dmapreduce.job.reduces=%s -Dmapreduce.map.memory.mb=%s %s %s >%s 2>&1 ; echo $? > %s" % (setting['hadoop_dir'], setting['hadoop_dir'], setting['num_reducers'], setting['map_size'], setting['dataset'], setting['job_output'], setting['job_log'], setting['job_returncode'])
		num_mapper = setting['job_size'] / 64
		num_reducer = num_mapper / 8 if num_mapper/8 > 0 else 1
		cmd = "%s/bin/hadoop jar %s/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar classification -m %s -r %s %s %s >%s 2>&1 ; echo $? > %s" % (setting['hadoop_dir'], setting['hadoop_dir'], num_mapper, num_reducer, setting['dataset'], setting['job_output'], setting['job_log'], setting['job_returncode'])
	return cmd

def submit_custom(command, task_log):
	cmd = Command(command).run()
	job_ids = mylog.lookup_job_ids(task_log)
	return job_ids

def submit_async(setting):
        cmd = generate_command(setting)
        print cmd
	os.system("mkdir -p %s" % setting['log_dir'])
	os.system(" (%s) &" % cmd)
	return setting
		
def submit(setting):
	cmd = generate_command(setting)	
	print cmd
	os.system("mkdir -p %s" % setting['log_dir'])
	Command(cmd).run()
	return setting

def submit_multiple(setting_list):
	for setting in setting_list:
		submit_async(setting)
	wait_completion(setting_list)

def wait_completion(setting_list):
	check_times = 0
	while True:
		count = 0
		all_pass = True
                for setting in setting_list:
                	if not os.path.exists(setting['job_returncode']):
                        	all_pass = False
			else:
				count = count + 1
		icon = "*" if check_times%2 == 0 else "+"	
		sys.stdout.write("\r%s Job progress -> submitted=%s, completed=%s" % (icon, len(setting_list), count))
		sys.stdout.flush()
		
                if all_pass:
                	break
                sleep(1)
		check_times = check_times + 1

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
