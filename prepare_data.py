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
from retrieve_task_info import *
import myjob
import myinfo
import mycluster
import mylog

def prepare(user):
	size_list = ["64MB", "128MB", "256MB", "512MB", "1GB", "2GB", "4GB"]
	dataset_dir = "/home/%s/dataset" % user
	hadoop_dir = "/home/%s/hadoop" % user
	for size in size_list:
		cmd = "%s/bin/dfs -put %s/wikipeida_%s /dataset" % (hadoop_dir, dataset_dir, size)
		Command(cmd).run()
	for size in size_list:
		real_size = myjob.convert_unit(size)
		num_rows = real_size / (1024 * 1024) / 100
		num_files = 1 if real_size <= 1024 else real_size/1024
		output_dir = "/dataset/terasort_%s" % size
		cmd = "%s/bin/hadoop jar %s/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar teragen -Dmapreduce.job.maps=%s %s  %s" % (hadoop_dir, hadoop_dir, num_files, num_rows, output_dir)
		Command(cmd).run()

def main(argv):
	parser = argparse.ArgumentParser(description='Configuration generator')
	parser.add_argument("-u", "--user", required=True, help="The user that owns the hadoop directory)
	args = parser.parse_args()
	prepare(args.user)

if __name__ == "__main__":
        main(sys.argv[1:])
