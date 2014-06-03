#!/usr/bin/python

import sys
import argparse
import env

env.init(debug=False)

from my.experiment.base import *
from my.experiment import jobfactory
from my.hadoop import config


def measure(job_list, job_size_list, output_dir, cluster_config_path, node_config_path, scheduler, parameters):

    parameters = config.parse_config(parameters)

    setting = HadoopSetting(cluster_config_path, node_config_path, scheduler=scheduler, model="reference", num_nodes=4, num_storages=4, parameters=parameters)

    map_size = 1024
    submit_times = 1
    job_timeline = jobfactory.create_all_pair_jobs(job_list=job_list, job_size_list=job_size_list, map_size=map_size, submit_times=submit_times)
    experiment = ExperimentRun(job_timeline, output_dir, setting=setting, upload=True, format=True, sync=True)

    experiment.init()
    experiment.run()


def main(argv):
    parser = argparse.ArgumentParser(description='Hadoop experiment')
    parser.add_argument("--directory", required=True, help="The output directory")
    parser.add_argument("--scheduler", required=False, default="Fifo", help="The scheduler")
    parser.add_argument("--job", required=True, choices=["terasort", "grep"], action="append", help="The job name")
    parser.add_argument("--size", required=True, action="append", help="The job size")
    parser.add_argument("--cluster", required=False, default="setting/cluster_config.py", help="The path to the cluster configuration")
    parser.add_argument("--node", required=False, default="setting/node_config.py", help="The path to the node configuratoin")
    parser.add_argument("-p", "--parameter", action="append", help="The parameters")
    args = parser.parse_args()

    measure(args.job, args.size, args.directory, args.cluster, args.node, args.scheduler, args.parameter)

if __name__ == "__main__":
    main(sys.argv[1:])
