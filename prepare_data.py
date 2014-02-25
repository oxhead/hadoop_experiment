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

def prepare(user, data_type, dataset_dir=None):
	#size_list = ["64MB", "128MB", "256MB", "512MB", "1GB", "2GB", "4GB"]
	size_list = ["2GB", "4GB"]
	dataset_dir = "/home/%s/dataset" % user if dataset_dir is None else dataset_dir
	hadoop_dir = "/home/%s/hadoop" % user
	if data_type == "wikipedia":
		for size in size_list:
			cmd = "%s/bin/hadoop dfs -put %s/wikipedia_%s /dataset" % (hadoop_dir, dataset_dir, size)
			print cmd
			Command(cmd).run()
	elif data_type == "terasort":
		for size in size_list:
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
	parser.add_argument("-t", "--kind", required=True, choices=["wikipedia", "terasort"], help="The dataset to create")
	parser.add_argument("-d", "--dataset", required=False, help="The dataset directory")
	args = parser.parse_args()
	prepare(args.user, args.kind, args.dataset)

if __name__ == "__main__":
        main(sys.argv[1:])
