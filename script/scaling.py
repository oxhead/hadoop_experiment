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
        'dfs.blocksize': '64m'
    }

    cluster_config_path = "setting/cluster_config.py"
    node_config_path = "setting/node_config.py"

    submit_times = 1
    block_size = parameters['dfs.blocksize'][:-1] + "MB"
 
    #io_buffer_list = ['65536', '16384', '4096']
    io_buffer_list = ['1048576', '67108864']
    job_list = ["grep", 'wordcount', 'terasort']
    job_size_list = ["1GB", "2GB", "4GB"]
    memory_list = ['32900', '28800', '24700', '20700', '16600', '12500', '8400', '4200']
    #memory_list = ['12500', '8400', '4200']
    #job_list = ['grep']
    #job_size_list = ['1GB']
    #memory_list = ['16789']
    failed = []
    for io_buffer in io_buffer_list:
	    for memory in memory_list:
		for job in job_list:
		    env.log_info("@ start experiment with job %s and memory size %s" %  (job, memory))   

                    # parameters
		    id = "scaling_%sM_%s_%s" % (memory, job, datetime.datetime.now().strftime("%Y%m%d%H%M"))
		    experiment_output_dir = output_dir + "/%s" % id
		    log_dir = "%s/log" % experiment_output_dir
		    os.makedirs(log_dir)
		    parameters['yarn.nodemanager.resource.memory-mb'] = memory
                    parameters['io.file.buffer.size'] = io_buffer                   
                    io_buffer_label = "%sKB" % (int(parameters['io.file.buffer.size']) / 1024)

		    job_timeline = jobfactory.create_all_pair_jobs(job_list=[job], job_size_list=job_size_list, submit_times=submit_times, log_dir=log_dir)
		    setting = HadoopSetting(cluster_config_path, node_config_path, scheduler="Fifo", model=model, num_nodes=num_nodes, num_storages=num_storages, parameters=parameters)
		    description = "purpose=scaling, nodes=1c1s, memory=%s, job=%s, block_size=%s, io_buffer=%s" % (memory, job, block_size, io_buffer_label)
		    finished = False
		    retries = 0;
		    while not finished and retries < 3:
			retries = retries + 1
			try:
			    experiment = ExperimentRun(job_timeline, experiment_output_dir, setting=setting, upload=True, format=True, sync=True, id=id, description=description, export=True)
			    experiment.init()
			    experiment.run()
			    finished = True
			except Exception as e:
			    env.log_exception('Unable to finish experiment')
			    env.log_info('Experiment failed, retrying')

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
