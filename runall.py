#!/usr/bin/python

import sys
import os
import argparse
from time import sleep
import tempfile
import datetime
import time
from history import *
from command import *

map_size = 1024

job_input_table = {
	"grep": "wikipedia_%s",
	"wordcount": "wikipedia_%s",
	"terasort": "terasort_%s",
	"networkintensive": "wikipedia_%s"
}

node_list = ["power3.csc.ncsu.edu",
	     "power4.csc.ncsu.edu",
    	     "power5.csc.ncsu.edu",
	     "power6.csc.ncsu.edu"]

ganglia_node_list = [
	"power2.csc.ncsu.edu",
	"power3.csc.ncsu.edu",
	"power4.csc.ncsu.edu",
	"power5.csc.ncsu.edu",
	"power6.csc.ncsu.edu",
]

collect_metric_list = ["cpu", "memory", "network"]
collect_metric_table = {"cpu": ["cpu_user", "cpu_system", "cpu_wio", "cpu_idle", "cpu_aidle", "cpu_nice"],
			"memory": ["mem_free", "mem_cached", "mem_buffers", "mem_shared"],
			"network": ["bytes_in", "bytes_out"]
			}

scheduler_table = {
	"InMemory": "org.apache.hadoop.yarn.server.resourcemanager.scheduler.im.InMemoryScheduler",
	"Capacity": "org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler",
	"Fifo": "org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler",
	"Fair": "org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler",
	"Flow": "org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow.FlowScheduler",
}

conf_table = {
	"yarn.inmemory.prefetch.transfer": "True",
	"io.file.buffer.size": "65536",
	"yarn.nodemanager.resource.memory-mb": "50000",
	"yarn.resourcemanager.scheduler.class": scheduler_table["InMemory"],
	"yarn.inmemory.enabled": "True",
	"fs.defaultFS": "file:///nfs_power2/", 
	"mapreduce.job.reduces": "1",
}


def batch(hadoop_dir, output_dir, nfs="/nfs_power2", model_list=["decoupled"], job_list=[], scheduler="InMemory", slot=48):
	for model in model_list:

		now = datetime.datetime.now()
		now_string = now.strftime("%Y%m%d%H%M")
		output_dir = os.path.join(output_dir, "%s_%s_%s_%s" % (model, slot, scheduler, now_string))

		data_dir = os.path.join(output_dir, "data")
		log_dir = os.path.join(output_dir, "log")
		execute_command("mkdir -p %s" % data_dir)
		execute_command("mkdir -p %s" % log_dir)
		clean_environment()
                switch_model(model=model, scheduler=scheduler, slot=slot)
               	start_hadoop(model=model)
                sleep(60)

		job_log_list = []

		time_start = int(time.time())
		for i in range(len(job_list)):
			job = job_list[i]
			job_type = job['type']
			job_size = job['size']
			job_reducer = job['reducer']
			#job_option = job['option']

			log_file = os.path.join(log_dir, "%s_%s.log" % (i, job_type))

			intput = "dataset/%s" % (job_input_table[job_type] % job_size)
                        output = "output/%s_%s_%s" % (job_type, job_input_table[job_type] % job_size, int(time.time()))

			submit_hadoop_job(job_type, intput, output, numOfReducer = job_reducer, log_file=log_file, model=model, nfs=nfs, async=True)
			#submit_hadoop_job(job_type, intput, output, numOfReducer = job_reducer, pattern = job_option, log_file=log_file, model=model, nfs=nfs, async=True)
			sleep(2)

			job_log_list.append(log_file)
		
		completion = 0
		while completion < len(job_log_list):
			completion = 0
			for job_log in job_log_list:
                		cmd = Command("grep 'completed successfully' %s | head -n 1 | cut -d' ' -f6" % job_log)
                		cmd.run()
                		job_id = cmd.output.strip()
				if len(job_id) > 0:
					print "Complete: %s" % job_log
					completion = completion + 1
			sleep(10)
		
		time_end = int(time.time())
			
		collect_data("power1.csc.ncsu.edu", time_start, time_end, data_dir)

		start_history_server("power6.csc.ncsu.edu")
		sleep(10)
		
		try:
			collect_hadoop_data("power6.csc.ncsu.edu", job_log_list, "%s/hadoop" % output_dir)
		except Exception as e:
			print e
			pass

		stop_history_server("power6.csc.ncsu.edu")
		stop_hadoop(model=model)


def remote_command(host, cmd):
	remote_cmd = "ssh %s \"%s\"" % (host, cmd)
	execute_command(remote_cmd)

'''the dir path should be absolute path'''
def remote_copy(host, dir_from, dir_to):
	remote_cmd = "scp -r %s:%s/* %s" % (host, dir_from, dir_to)
	execute_command(remote_cmd)

def collect_hadoop_data(history_server, job_log_list, output_dir):
	execute_command("mkdir -p %s" % output_dir)
	job_list = []
	for job_log in job_log_list:
		print job_log
		cmd = Command("grep 'completed successfully' %s | head -n 1 | cut -d' ' -f6" % job_log)
		cmd.run()
                job_id = cmd.output.strip()
                print '@@ job id=%s' % job_id
		print job_log
                job_list.append(job_id)
                file_runtime = os.path.join(output_dir, "runtime_%s.csv" % os.path.basename(job_log).replace(".log", ""))
		print job_id, file_runtime
                fetch_job_info(history_server, "19888", job_id, file_runtime)

	file_distribution = os.path.join(output_dir, "timeline.csv")
	fetch_jobs(history_server, "19888", job_list, file_distribution) 
		

def collect_data(ganglia_host, time_start, time_end, output_dir):
	rrds_dir = "/var/lib/ganglia/rrds"
	cluster_name = "power"
	ganglia_host = "power1.csc.ncsu.edu"
	remote_tmp_dir = os.path.join("/tmp", "rrds_%s_%s" % (cluster_name, int(time.time())))
	# how mnay seconds
	collect_step = 5
	for node in ganglia_node_list:
		remote_node_dir = os.path.join(remote_tmp_dir, node)
		remote_command(ganglia_host, "mkdir -p %s" % remote_node_dir)
		for metric in collect_metric_list:
			remote_output_file = os.path.join(remote_node_dir, metric)
			rrd_list = ["DEF:%s=%s/%s/%s/%s.rrd:sum:AVERAGE XPORT:%s:'%s'" % (rrd_metric, rrds_dir, cluster_name, node, rrd_metric, rrd_metric, rrd_metric) for rrd_metric in collect_metric_table[metric]]
			rrd_list_string = " ".join(rrd_list)
			cmd = "rrdtool xport --start %s --end %s %s --step %s > %s" % (time_start, time_end, rrd_list_string, collect_step, remote_output_file)
			remote_command(ganglia_host, cmd)
	remote_copy(ganglia_host, remote_tmp_dir, output_dir)
			

def submit_hadoop_job(job, input, output, numOfReducer=16, pattern="kinmen.*", model="decoupled", log_file=None, nfs="/nfs_power2", async=False):
	
	if "decoupled" in model:
		input = "file://%s/%s" % (nfs, input)
		output = "file://%s/%s" % (nfs, output)
	elif "reference" in model:
		input = "/%s" % (input)
                output = "/%s" % (output)

	cmd = ""
	log_file = tempfile.NamedTemporaryFile(delete=False) if log_file is None else log_file
	if job == "grep":
		cmd = "~/hadoop/bin/hadoop jar ~/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar grep -Dmapreduce.map.memory.mb=%s -Dmapreduce.job.reduces=%s %s %s %s > %s 2>&1" % (map_size, numOfReducer, input, output, pattern, log_file)
	elif job == "wordcount":
		cmd = "~/hadoop/bin/hadoop jar ~/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar wordcount -Dmapreduce.map.memory.mb=%s -Dmapreduce.job.reduces=%s %s %s > %s 2>&1" % (map_size, numOfReducer, input, output, log_file) 
	elif job == "terasort":
		cmd = "~/hadoop/bin/hadoop jar ~/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar terasort -Dmapreduce.map.memory.mb=%s -Dmapreduce.job.reduces=%s %s %s > %s 2>&1" % (map_size, numOfReducer, input, output, log_file)
	elif job == "networkintensive":
                cmd = "~/hadoop/bin/hadoop jar ~/HadoopBenchmark.jar my.oxhead.hadoop.benchmark.NetworkIntensiveJob -Dmapreduce.map.memory.mb=%s -Dmapreduce.job.reduces=%s %s %s > %s 2>&1" % (map_size, numOfReducer, input, output, log_file)

	if async:
		execute_command_in_background(cmd)
	else:
		execute_command(cmd)

def generate_conf(dir, model="decoupled", scheduler="InMemory", slot=48):
	mb = slot * 1024 + 512 
	buffer = 65536
	fs = ""
	if "decoupled" in model:
		fs = "file:///nfs_power2/"
	else:
		fs = "hdfs://power6.csc.ncsu.edu:18020/"
	scheduler_type = scheduler_table[scheduler]
	prefetch_enabled = "true" if "InMemory" in scheduler_type else "false"

	conf_table["io.file.buffer.size"] = buffer
	conf_table["yarn.nodemanager.resource.memory-mb"] = mb
	conf_table["fs.defaultFS"] = fs
	conf_table["yarn.resourcemanager.scheduler.class"] = scheduler_type
	conf_table["yarn.inmemory.enabled"] = prefetch_enabled

	cmd = "python2.7 generate_configuration.py -c conf -d %s" % dir
	for k, v in conf_table.items():
		cmd = cmd + " -p %s=%s" % (k, v)
	
	#cmd = "python2.7 generate_configuration.py -c conf -d %s -p io.file.buffer.size=%s -p yarn.nodemanager.resource.memory-mb=%s -p fs.defaultFS=%s -p yarn.resourcemanager.scheduler.class=%s -p yarn.inmemory.enabled=%s" % (dir, buffer, mb, fs, scheduler_type, prefetch_enabled)
	execute_command(cmd)

def clean_environment():
	for node in ganglia_node_list:
		cmd = "ssh %s 'sudo ntpdate pool.ntp.org; sudo sync; echo 3 | sudo tee /proc/sys/vm/drop_caches'" % node
		execute_command(cmd)

def switch_model(model="decoupled", scheduler="InMemory", slot=48):
	print "Switch Hadoop model: %s" % model
	conf_dir = "/tmp/conf_%s" % model
	generate_conf(conf_dir, model, scheduler, slot)
	cmd_list = []
	for node in node_list:
		cmd_mkdir = "ssh %s mkdir -p ~/hadoop/conf" % (node)
		cmd_list.append(cmd_mkdir)
		cmd_cp = "scp -r %s/* %s:~/hadoop/conf" % (conf_dir, node)
		#cmd_ln = "ssh %s 'unlink ~/hadoop/conf && ln -s ~/conf ~/hadoop/conf'" % node
		cmd_list.append(cmd_cp)
		#cmd_list.append(cmd_ln)
	execute_commands(cmd_list)

def start_history_server(host):
        print "Start History Server: %s" % host
        cmd = "~/hadoop/sbin/mr-jobhistory-daemon.sh start historyserver --conf ~/hadoop/conf"
        execute_command(cmd)

def stop_history_server(host):
        print "Stop History Server: %s" % host
        cmd = "~/hadoop/sbin/mr-jobhistory-daemon.sh stop historyserver"
        execute_command(cmd)

def start_hadoop(model="decoupled"):
	print "Start Hadoop model: %s" % model
	if "decoupled" not in model:
		cmd_dfs = "~/hadoop/sbin/start-dfs.sh"
		execute_command(cmd_dfs)
		sleep(60)

	cmd_yarn = "~/hadoop/sbin/start-yarn.sh"
	execute_command(cmd_yarn)

def stop_hadoop(model="decoupled"):
	print "Stop Hadoop model: %s" % model
	cmd_list = []

        cmd_yarn = "~/hadoop/sbin/stop-yarn.sh"

	if "decoupled" not in model:
                cmd_dfs = "~/hadoop/sbin/stop-dfs.sh"
                cmd_list.append(cmd_dfs)

        cmd_list.append(cmd_yarn)
	execute_commands(cmd_list)

def execute_commands(cmd_list):
	for cmd in cmd_list:
		execute_command(cmd)

def execute_command(cmd):
	print cmd
	os.system(cmd)

def create_job_list(job_type_list, size_list, reducer_list, option_list):
	job_list = []
	for i in range(len(job_type_list)):
		job = {"type": job_type_list[i], "size": size_list[i], "reducer": reducer_list[i]}
		job_list.append(job)
	return job_list
		

def main(argv):

        input_dir = ''
        output_dir = ''

        parser = argparse.ArgumentParser(description='Hadoop job submitter')

        parser.add_argument('--hadoop', required=True, help="The Hadoop directory")
	parser.add_argument('--nfs', required=True, help="The nfs directory")
        parser.add_argument("-d", '--directory', required=True, help="The output directory")
	parser.add_argument("--model", action="append", required=True, help="The Hadoop model to run")
	parser.add_argument("-t", '--scheduler', default="InMemory", choices=scheduler_table.keys(), help="The Hadoop scheduler")
	parser.add_argument("-n", '--slot', type=int, default=48, help='The number of slots')
	parser.add_argument("--transfer", action='store_true', help='Enable data transfer')
	parser.add_argument("--window", default="48", help='The size of prefetch window')
	parser.add_argument("--concurrency", default="48", help="The number of concurrent prefeching tasks")
	parser.add_argument("--pdir", default="/dev/shm/hadoop", help="The location of prefetch blocks")

	group = parser.add_argument_group('job')
	group.add_argument("--job", action="append", required=True, help="The Hadoop jobs to run")
	group.add_argument("-s", '--size', action="append", help="The job input size with units, e.g. MB or GB")
	group.add_argument("--reducer", action="append", help='The number of reducer')
	group.add_argument("--option", action="append", help='The option for map')

        args = parser.parse_args()
	job_list = create_job_list(args.job, args.size, args.reducer, args.option)

	conf_table["yarn.inmemory.prefetch.transfer"] = str(args.transfer)
	conf_table["mapreduce.job.reduces"] = "16"
	conf_table["yarn.inmemory.prefetch.window"] = args.window
	conf_table["yarn.inmemory.prefetch.concurrency"] = args.concurrency
	conf_table["yarn.inmemory.prefetch.tasks"] = str(args.slot)
	conf_table["yarn.inmemory.prefetch.dir"] = args.pdir
        batch(args.hadoop, args.directory, model_list = args.model, nfs=args.nfs, job_list = job_list, scheduler=args.scheduler, slot=args.slot)

if __name__ == "__main__":
        main(sys.argv[1:])
