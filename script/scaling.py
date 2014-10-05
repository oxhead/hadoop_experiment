#!/usr/bin/python
import sys
import argparse
import env

from my.experiment.base import *
from my.experiment import jobfactory

@env.default
def measure(model, schedulers, num_nodes, num_storages, submit_times, submit_ratio, output_dir, debug=False):

    if debug:
        env.enable_debug()

    parameters = {
        'mapreduce.job.reduce.slowstart.completedmaps': '1.0',
        'dfs.replication': '1',
    }

    cluster_config_path = "setting/cluster_config.py"
    node_config_path = "setting/node_config.py"

    job_list = ["terasort", "grep"]
    job_size_list = ["1GB", "2GB"]
    submit_times = 1
    job_timeline = jobfactory.create_all_pair_jobs(job_list=job_list, job_size_list=job_size_list, submit_times=submit_times)
    #memory_list = ['4200', "8400", "12500", '16600']
    memory_list = ['4200', '8600']
    for memory in memory_list:
        env.log_info("@ start experiment with memory size %s" %  memory)   
        parameters['yarn.nodemanager.resource.memory-mb'] = memory
        setting = HadoopSetting(cluster_config_path, node_config_path, scheduler="Fifo", model=model, num_nodes=num_nodes, num_storages=num_storages, parameters=parameters)
        experiment = ExperimentRun(job_timeline, output_dir, setting=setting, upload=True, format=True, sync=True)
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
