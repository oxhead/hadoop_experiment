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

import myjob
import myinfo
import mylog

def measure(size, map_size=1024):
	setting_list = []
	# per GB
	for i in range(size):

		setting = myjob.get_job_setting(job="custommap", prefix="computing-flow-%s" % i, job_size=1024, map_size=map_size)
		setting_list.append(setting)

		cmd = Command("%s/bin/hadoop jar %s/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar custommap -Dmapreduce.job.reduces=1 -Dmapreduce.map.memory.mb=%s %s %s 0 1 1 1024 > %s 2>&1 ; echo $? > %s" % (setting['hadoop_dir'], setting['hadoop_dir'], setting['map_size'], setting['dataset'], setting['job_output'], setting['job_log'], setting['job_returncode']) )

		print cmd.command
                os.system(" (%s) &" % cmd.command)
	
	myjob.wait_completion(setting_list)

	job_id_list = []
	for setting in setting_list:
		job_log = setting['job_log']
                job_ids = mylog.lookup_job_ids(job_log)
                job_id_list.extend(job_ids)

	(start_time, end_time) = myinfo.get_elapsed_time(job_id_list)
        elapsed_time = end_time- start_time

	print "elapsed time: %s sec" % (elapsed_time/1000.0)
	print "computing flow capability: %s MB/s" % (1024 * size /(elapsed_time/1000.0))



def main(argv):
	parser = argparse.ArgumentParser(description='Configuration generator')
	parser.add_argument("-s", "--size", type=int, required=True, help="The size varies from 1 to the specified GB")
	parser.add_argument("-m", "--map", type=int, required=False, default=1024, help="The memory usage of each map with the unit MB")

	args = parser.parse_args()
	measure(args.size, args.map)

if __name__ == "__main__":
        main(sys.argv[1:])
