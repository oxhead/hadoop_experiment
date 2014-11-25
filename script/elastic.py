#!/usr/bin/python
import sys
import argparse
import random
import env

from my.experiment.base import *
from my.experiment import jobfactory
from my.util import command

@env.default
def measure(model, schedulers, num_nodes, num_storages, submit_times, submit_ratio, output_dir, debug=False):

    env.enable_debug()
    if debug:
        env.enable_debug()

    parameters = {
        'mapreduce.job.reduce.slowstart.completedmaps': '1.0',
        'dfs.replication': '3',
        'dfs.blocksize': '64m'
    }

    cluster_config_path = "setting/cluster_config_reference.py"
    node_config_path = "setting/node_config.py"

    submit_times = 5
    io_buffer = '65536'
    job = 'grep'
    job_size = '4GB'
    memory = '16600'

    env.log_info("@ start experiment with job %s and memory size %s" %  (job, memory))
    id = "elstic_%sM_%s_%s" % (memory, job, datetime.datetime.now().strftime("%Y%m%d%H%M%S"))
    experiment_output_dir = output_dir + "/%s" % id
    log_dir = "%s/log" % experiment_output_dir
    os.makedirs(log_dir)
    parameters['yarn.nodemanager.resource.memory-mb'] = memory
    parameters['io.file.buffer.size'] = io_buffer                   
    io_buffer_label = "%sKB" % (int(parameters['io.file.buffer.size']) / 1024)
    block_size = parameters['dfs.blocksize'][:-1] + "MB"
    
    job_timeline = jobfactory.create_all_pair_jobs(job_list=[job], job_size_list=[job_size], submit_times=submit_times, log_dir=log_dir)
    setting = HadoopSetting(cluster_config_path, node_config_path, scheduler="Capacity", model=model, num_nodes=num_nodes, num_storages=num_storages, parameters=parameters)
    description = "purpose=elastic, nodes=1c1s, memory=%s, job=%s, block_size=%s, io_buffer=%s" % (memory, job, block_size, io_buffer_label)
    experiment = ExperimentRun(job_timeline, experiment_output_dir, setting=setting, upload=True, format=True, sync=True, id=id, description=description, export=True)
    #random_kill_nodemanager(experiment.cluster)
    kill_two_nodemanager(experiment.cluster)
    experiment.init()
    experiment.run()

def random_kill_nodemanager(cluster):
    nodemanagers = cluster.getMapReduceCluster().getNodeManagers()
    node = nodemanagers[random.randint(0, len(nodemanagers) - 1)]
    print "@@@@@@@@@@ %s " % node.host
    command.execute("( sleep 150; echo stop service; python bin/node.py -n %s -r nodemanager -a kill ) &" % node.host, output=True)

def kill_two_nodemanager(cluster):
    nodemanagers = cluster.getMapReduceCluster().getNodeManagers()
    node = nodemanagers[0]
    print "@@@@@@@@@@ %s " % node.host
    command.execute("( sleep 150; echo stop service; python bin/node.py -n %s -r nodemanager -a kill ) &" % node.host, output=True)
    node = nodemanagers[1]
    print "@@@@@@@@@@ %s " % node.host
    command.execute("( sleep 200; echo stop service; python bin/node.py -n %s -r nodemanager -a kill ) &" % node.host, output=True)

def main(argv):
    parser = argparse.ArgumentParser(description='Hadoop experiment')
    parser.add_argument("-d", "--directory", default='output', help="The output directory")
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
