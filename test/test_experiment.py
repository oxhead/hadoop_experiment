#!/usr/bin/python
import os
LIB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), "src")

import sys
sys.path.append(LIB_PATH)

import time

from my.experiment.base import *
from my.experiment import util
from my.experiment import monitoringtool
from my.hadoop import config

# Test class
def test_create_job():
	print "[Testing] creat job"
	hadoop_dir = "~/hadoop"
	name = "Grep"
	size = 1024
	intput_dir = "/dataset/wikipedia_1GB"
	output_dir = "/output/test_1"
	log = "log/test_1.log"
	params=None
	map_size=1024
	num_reducers=1
	job = Job(hadoop_dir, name, size, intput_dir, output_dir, log, params, map_size, num_reducers)
	print job

def test_switch_config():
	print "[Testing] switch hadoop configuration"
	config_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
	conf_dir = os.path.join(config_path, "conf")
	cluster_config_path = os.path.join(config_path, "config/cluster_config.py")
	node_config_path = os.path.join(config_path, "config/node_config.py")
	scheduler = "ColorStorage"
	format = False
	util.switch_config(conf_dir, cluster_config_path, node_config_path, scheduler, format)

def test_monitoring():
	print "[Testing] collecting data"
	#config_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
	config_path = os.getcwd()
	cluster_config_path = os.path.join(config_path, "config/cluster_config.py")
	cluster = config.get_cluster(cluster_config_path)
	output_dir = os.path.join(config_path, "results/bb")
	monitor = monitoringtool.Monitor(cluster, output_dir)
	monitor.start()
	time.sleep(10)
	monitor.stop()


def main(argv):
	#test_create_job()
	#test_switch_config()
	test_monitoring()
        
if __name__ == "__main__":
        main(sys.argv[1:])