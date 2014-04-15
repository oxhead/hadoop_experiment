#!/usr/bin/python
import os
LIB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), "src")
PROJECT_PATH = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

import sys
sys.path.append(LIB_PATH)

import logging

from my.experiment.base import *
from my.experiment import jobfactory

def test():
	logging.getLogger("requests").setLevel(logging.CRITICAL)
	logging.basicConfig(level=logging.DEBUG)

	output_dir = os.path.join(PROJECT_PATH, "results/abc")

	scheduler="Fifo"
	model="decoupled"
	num_nodes=1
	num_storages=1
	parameters={}
	upload = True
	format = True

	cluster_config_path = os.path.join(PROJECT_PATH, "config", "cluster_config.py")
	node_config_path = os.path.join(PROJECT_PATH, "config", "node_config.py")
	setting = HadoopSetting(cluster_config_path, node_config_path, scheduler=scheduler, model=model, num_nodes=num_nodes, num_storages=num_storages, parameters=parameters)



	job_list = ["grep", "terasort", "wordcount"]
	job_size_list = ["1GB"]
	submit_times = 3
	job_timeline = jobfactory.create_fixed_jobs(job_list=job_list, job_size_list=job_size_list, submit_times=submit_times)

	experiment = ExperimentRun(job_timeline, output_dir, setting=setting, upload=upload, format=format)
	experiment.init()
	experiment.run()


def main(argv):
	test()

if __name__ == "__main__":
        main(sys.argv[1:])
