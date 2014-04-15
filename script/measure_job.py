#!/usr/bin/python
#!/usr/bin/python
import os
import sys
import argparse
import env

env.init()
from my.experiment.base import *
from my.experiment import jobfactory


def measure(output_dir):

    scheduler="Fifo"
    model="decoupled"
    num_nodes=1
    num_storages=1
    parameters={}
    upload = True
    format = True

    cluster_config_path = env.get_cluter_config_path(model, num_nodes, num_storages)
    node_config_path = env.get_node_config_path()
    setting = HadoopSetting(cluster_config_path, node_config_path, scheduler=scheduler, model=model, num_nodes=num_nodes, num_storages=num_storages, parameters=parameters)



    job_list = ["grep", "terasort", "wordcount", "nocomputation", "histogrammovies", "histogramratings", "custommap1"]
    #job_list = ["nocomputation", "histogrammovies", "histogramratings", "custommap1"]
    job_size_list = ["64MB"]
    submit_times = 1
    job_timeline = jobfactory.create_all_pair_jobs(job_list=job_list, job_size_list=job_size_list, submit_times=submit_times)
    experiment = ExperimentRun(job_timeline, output_dir, setting=setting, upload=upload, format=format, sync=True)
    experiment.init()
    experiment.run()


def main(argv):
    parser = argparse.ArgumentParser(description='Hadoop experiment')
    parser.add_argument("-d", "--directory", required=True, help="The output directory")
    args = parser.parse_args()

    measure(args.directory)

if __name__ == "__main__":
        main(sys.argv[1:])
