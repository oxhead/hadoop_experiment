#!/usr/bin/python
import sys
import argparse
import env

env.init()

from my.experiment.base import *
from my.experiment import jobfactory

def measure(model, schedulers, num_nodes, num_storages, submit_times, submit_ratio, output_dir, debug=False):

    if debug:
        env.enable_debug()

    parameters = {
        'mapreduce.job.reduce.slowstart.completedmaps': '1.0',
        'dfs.replication': '1',
    }

    cluster_config_path = env.get_cluter_config_path(model, num_nodes, num_storages)
    node_config_path = env.get_node_config_path()
    setting = HadoopSetting(cluster_config_path, node_config_path, scheduler="Fifo", model=model, num_nodes=num_nodes, num_storages=num_storages, parameters=parameters)

    job_list = ["grep", "histogramratings"]
    job_size = "4GB"
    job_timeline = jobfactory.create_ab_jobs(job_list=job_list, job_size=job_size, submit_times=submit_times, submit_ratio=submit_ratio)

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
    parser.add_argument("--submit_times", type=int, default=1, help="The times of job submition")
    parser.add_argument("--submit_ratio", type=int, default=1, help="The ratio of job A to job B")
    parser.add_argument("--debug", action='store_true', help="Turn on the debug mode")
    args = parser.parse_args()

    measure(args.model, args.scheduler, args.num_nodes, args.num_storages, args.submit_times, args.submit_ratio, args.directory, args.debug)

if __name__ == "__main__":
    main(sys.argv[1:])
