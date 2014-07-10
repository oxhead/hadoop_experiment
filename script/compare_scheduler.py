#!/usr/bin/python

import sys
import argparse
import env

env.init()

from my.experiment.base import *
from my.experiment import jobfactory


def measure(model, schedulers, num_jobs, period, output_dir,  cluster_config_path, node_config_path, parameters):

    parameters = config.parse_config(parameters)

    setting = HadoopSetting(cluster_config_path, node_config_path, scheduler="Fifo", model=model, parameters=parameters)

    #job_list = ["grep", "terasort", "wordcount", "nocomputation", "histogrammovies", "histogramratings", "custommap1"]
    job_list = ["grep", "terasort", "wordcount"]
    job_size_list = ["1GB", "2GB", "4GB"]
    job_timeline = jobfactory.create_jobs(job_list=job_list, job_size_list=job_size_list, num_jobs=num_jobs, period=period)

    experiment = SchedulerExperiment(
        setting, schedulers, job_timeline, output_dir)

    experiment.init()
    experiment.run()


def main(argv):
    parser = argparse.ArgumentParser(description='Hadoop experiment')
    parser.add_argument("-d", "--directory", required=True, help="The output directory")
    parser.add_argument("-m", "--model", default="decoupled", choices=["decoupled", "reference"])
    parser.add_argument("-s", "--scheduler", action='append', help="The scheduler")
    parser.add_argument("--num_jobs", type=int, default=10, help="The number of jobs")
    parser.add_argument("--period", type=int, default=10, help="The number of jobs")
    parser.add_argument("--cluster", required=False, default="setting/cluster_config.py", help="The path to the cluster configuration")
    parser.add_argument("--node", required=False, default="setting/node_config.py", help="The path to the node configuratoin")
    parser.add_argument("-p", "--parameter", action="append", help="The parameters")
    args = parser.parse_args()

    measure(args.model, args.scheduler, args.num_jobs, args.period, args.directory, args.cluster, args.node, args.parameter)

if __name__ == "__main__":
    main(sys.argv[1:])
