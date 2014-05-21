#!/usr/bin/python

import sys
import argparse
import env

def measure(model, schedulers, num_nodes, num_storages, job_list, num_jobs, output_dir, debug=False):

    env.init(debug=debug)

    from my.experiment.base import HadoopSetting
    from my.experiment.base import SchedulerExperiment
    from my.experiment import jobfactory

    parameters = {
        'mapreduce.job.reduce.slowstart.completedmaps': '0.8',
    }

    cluster_config_path = env.get_cluter_config_path(
        model, num_nodes, num_storages)
    node_config_path = env.get_node_config_path()
    setting = HadoopSetting(cluster_config_path, node_config_path, scheduler="Fifo", model=model, num_nodes=num_nodes, num_storages=num_storages, parameters=parameters)

    #job_list = ["grep", "terasort", "wordcount", "nocomputation", "histogrammovies", "histogramratings", "custommap1"]
    #job_list = ["grep"]
    job_size_list = ["1GB"]
    job_timeline = jobfactory.create_jobs(job_list=job_list, job_size_list=job_size_list, num_jobs=num_jobs, period=10)

    experiment = SchedulerExperiment(
        setting, schedulers, job_timeline, output_dir)

    experiment.init()
    experiment.run()


def main(argv):
    parser = argparse.ArgumentParser(description='Hadoop experiment')
    parser.add_argument("-d", "--directory", required=True, help="The output directory")
    parser.add_argument("-m", "--model", default="decoupled", choices=["decoupled", "reference"])
    parser.add_argument("-s", "--scheduler", action='append', help="The scheduler")
    parser.add_argument("-j", "--job", action='append', help="The jobs")
    parser.add_argument("--num_nodes", type=int, default=1, help="The number of computing nodes")
    parser.add_argument("--num_storages", type=int, default=1, help="The number of storage nodes")
    parser.add_argument("--num_jobs", type=int, default=10, help="The number of jobs")
    parser.add_argument("--debug", action='store_true', help="Turn on the debug mode")
    args = parser.parse_args()

    measure(args.model, args.scheduler, args.num_nodes, args.num_storages, args.job, args.num_jobs, args.directory, debug=args.debug)

if __name__ == "__main__":
    main(sys.argv[1:])
