#!/usr/bin/python
import sys
import argparse
import env
import datetime
import os

from my.experiment.base import ExperimentRun
from my.experiment.controller import Controller
from my.experiment import jobfactory
from my.hadoop.base import HadoopSetting
from my.hadoop import configtool
from my.util import jsonutil

def build_cluster(num_nodemanagers, num_datanodes):
    resourcemanager = "10.25.13.41"
    nodemanagers = ["10.25.13.31", "10.25.13.32", "10.25.13.37", "10.25.13.38"]
    namenode = "10.25.13.41"
    datanodes = ["10.25.13.34", "10.25.13.36"]
    historyserver = "10.25.13.41:19888"
    user = "root"

    return configtool.create_cluster(resourcemanager, nodemanagers[:num_nodemanagers], namenode, datanodes[:num_datanodes], historyserver, user)

def measure(model, schedulers, num_nodes, num_storages, submit_times, submit_ratio, output_dir, debug=False):

    env.init(debug=debug)

    # Parameters
    scheduler = "Fifo"
    parameters = {
        'mapreduce.job.reduce.slowstart.completedmaps': '0.05',
        'dfs.replication': '1',
        'dfs.blocksize': '64m',
    }
    io_buffer = '1048576'
    memory = '16600'
    parameters['yarn.nodemanager.resource.memory-mb'] = memory
    parameters['io.file.buffer.size'] = io_buffer
    configtool.set_scheduler(parameters, scheduler)

    # Hadoop setting
    num_computing = 4
    num_storage = 2
    cluster = build_cluster(num_computing, num_storage)
    config = configtool.parse_node_config("setting/node_config.py")
    setting = HadoopSetting(cluster, config, parameters=parameters)

    # Experiment and job related setting
    job_list = ['terasort', 'grep', 'wordcount']
    #job_list = ['grep']
    #job_size_list = ["64MB", "128MB", "256MB", "512MB", "1GB", "2GB"]
    job_size_list = ["128MB", "256MB", "512MB", "1GB", "2GB"]
    num_jobs = 40
    period = 360

    # Initializing Experiment
    id = "access-pattern_%sc%ss_%sM_%sjobs_%s_%s" % (num_computing, num_storage, memory, num_jobs, scheduler, datetime.datetime.now().strftime("%Y%m%d%H%M%S"))
    print id
    experiment_output_dir = output_dir + "/%s" % id
    io_buffer_label = "%sKB" % (int(parameters['io.file.buffer.size']) / 1024)
    block_size = parameters['dfs.blocksize'][:-1] + "MB"
    description = "purpose=characterization, nodes=%sc%ss, memory=%s, num_jobs=%s, block_size=%s, io_buffer=%s" % (num_computing, num_storage, memory, num_jobs, block_size, io_buffer_label)
    log_dir = experiment_output_dir + "/log"
    job_timeline = jobfactory.create_jobs(job_list=job_list, job_size_list=job_size_list, num_jobs=num_jobs, log_dir=log_dir)

    # Running the experiment 
    experiment = ExperimentRun(setting, job_timeline, experiment_output_dir, upload=True, format=True, sync=False, id=id, description=description, export=True)
    controller = Controller(experiment)
    #controller = Controller.from_snapshot("output/access-pattern_1c2s_16600M_3jobs_Fifo_20150504193744/snapshot/experiment.json", id=id, output_dir=experiment_output_dir)
    #controller.take_snapshot()
    #import sys
    #sys.exit()
    controller.run()

def main(argv):
    parser = argparse.ArgumentParser(description='Hadoop experiment')
    parser.add_argument("-d", "--directory", default="output", help="The output directory")
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
