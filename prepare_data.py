#!/usr/bin/python

import sys
import os
import argparse
from time import sleep
import tempfile
import datetime
import time
from command import *
from command import Command
import myjob

def prepare_multiple(user, data_type, dataset_dir, size_list):
	for size in size_list:
		prepare(user, data_type, dataset_dir, size)

def prepare(user, data_type, dataset_dir, size):
	dataset_dir = "/home/%s/dataset" % user if dataset_dir is None else dataset_dir
	hadoop_dir = "/home/%s/hadoop" % user
	if data_type == "wikipedia":
		cmd = "%s/bin/hadoop dfs -put %s/wikipedia_%s /dataset" % (hadoop_dir, dataset_dir, size)
		print cmd
		Command(cmd).run()
	elif data_type == "kmeans":
		cmd = "%s/bin/hadoop dfs -put %s/kmeans_%s /dataset" % (hadoop_dir, dataset_dir, size)
		print cmd
		Command(cmd).run()
	elif data_type == "terasort":
		real_size = myjob.convert_unit(size)
		num_rows = real_size * 1024 * 1024 / 100
		num_files = 1 if real_size <= 1024 else real_size/1024
		output_dir = "/dataset/terasort_%s" % size
		cmd = "%s/bin/hadoop jar %s/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar teragen -Dmapreduce.job.maps=%s %s  %s" % (hadoop_dir, hadoop_dir, num_files, num_rows, output_dir)
		print cmd
		os.system(cmd)

def main(argv):
	parser = argparse.ArgumentParser(description='Configuration generator')
	parser.add_argument("-u", "--user", required=True, help="The user that owns the hadoop directory")
	parser.add_argument("-t", "--kind", required=True, choices=["wikipedia", "terasort", "kmeans"], help="The dataset to create")
	parser.add_argument("-d", "--dataset", required=False, help="The dataset directory")
	parser.add_argument("-s", "--size", action="append", required=False, help="The dataset size")
	args = parser.parse_args()
	prepare_multiple(args.user, args.kind, args.dataset, args.size)

if __name__ == "__main__":
        main(sys.argv[1:])
