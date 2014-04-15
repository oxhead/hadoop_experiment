#!/usr/bin/python
import os
import sys
import argparse
import env

env.init()
from my.experiment.base import *
from my.experiment import jobfactory

def measure(model, scheduler, num_nodes, num_storages, output_dir):


    cluster_config_path = env.get_cluter_config_path(
        model, num_nodes, num_storages)
    node_config_path = env.get_node_config_path()
    setting = HadoopSetting(
        cluster_config_path, node_config_path, scheduler="Fifo",
                            model=model, num_nodes=num_nodes, num_storages=num_storages, parameters={})
    upload = True
    format = True

    job_list = ["nocomputation"]
    job_size_list = ["4GB"]
    map_size = 1024
    submit_times = 4
    job_timeline = jobfactory.create_fixed_jobs(job_list=job_list, job_size_list=job_size_list, map_size=map_size, submit_times=submit_times)
    experiment = ExperimentRun(job_timeline, output_dir, setting=setting, upload=upload, format=format)
    experiment.init()
    experiment.run()


def main(argv):
    parser = argparse.ArgumentParser(description='Hadoop experiment')
    parser.add_argument("-d", "--directory", required=True, help="The output directory")
    parser.add_argument("-m", "--model", default="decoupled", choices=["decoupled", "reference"])
    parser.add_argument("-s", "--scheduler", required=True, help="The scheduler")
    parser.add_argument("--num_nodes", type=int, default=1, help="The number of computing nodes")
    parser.add_argument("--num_storages", type=int, default=1, help="The number of storage nodes")
    args = parser.parse_args()

    measure(args.model, args.scheduler, args.num_nodes,
        args.num_storages, args.directory)

if __name__ == "__main__":
        main(sys.argv[1:])
