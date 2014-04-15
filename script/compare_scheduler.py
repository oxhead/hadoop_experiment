#!/usr/bin/python
import os
LIB_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.realpath(__file__))), "src")
PROJECT_PATH = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

import sys
sys.path.append(LIB_PATH)

import argparse
import logging

from my.experiment.base import *
from my.experiment import jobfactory

import env


def measure(model, schedulers, num_nodes, num_storages, num_jobs, period, output_dir):

    env.init()

    cluster_config_path = env.get_cluter_config_path(
        model, num_nodes, num_storages)
    node_config_path = env.get_node_config_path()
    setting = HadoopSetting(
        cluster_config_path, node_config_path, scheduler="Fifo",
                            model=model, num_nodes=num_nodes, num_storages=num_storages, parameters={})

    job_list = ["grep", "terasort", "wordcount", "nocomputation", "histogrammovies", "histogramratings", "custommap1"]
    job_size_list = ["1GB", "2GB", "4GB"]
    job_timeline = jobfactory.create_jobs(
        job_list=job_list, job_size_list=job_size_list, num_jobs=num_jobs, period=period)

    experiment = SchedulerExperiment(
        setting, schedulers, job_timeline, output_dir)

    experiment.init()
    experiment.run()


def main(argv):
    parser = argparse.ArgumentParser(description='Hadoop experiment')
    parser.add_argument("-d", "--directory", required=True, help="The output directory")
    parser.add_argument("-m", "--model", default="decoupled", choices=["decoupled", "reference"])
    parser.add_argument("-s", "--scheduler", action='append', help="The scheduler")
    parser.add_argument("--num_nodes", type=int, default=1, help="The number of computing nodes")
    parser.add_argument("--num_storages", type=int, default=1, help="The number of storage nodes")
    parser.add_argument("--num_jobs", type=int, default=10, help="The number of jobs")
    parser.add_argument("--period", type=int, default=10, help="The number of jobs")
    args = parser.parse_args()

    measure(args.model, args.scheduler, args.num_nodes,
        args.num_storages, args.num_jobs, args.period, args.directory)

if __name__ == "__main__":
    main(sys.argv[1:])
