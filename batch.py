#!/usr/bin/python

import sys
import os
import argparse
from time import sleep
import tempfile
import datetime
import time

map_size = 1024
job_list = ["grep", "wordcount", "terasort", "networkintensive"]
#job_list = ['terasort']
#job_list = ['networkintensive']
#model_list = ["decoupled", "reference"]
#model_list = ["decoupled"]
#model_list = ["reference"]

job_input_table = {"grep": "wikipedia_50GB",
		   "wordcount": "wikipedia_50GB",
		   "terasort": "terasort_50GB",
		   "networkintensive": "wikipedia_50GB"}
#job_input_table = {"terasort": "terasort_1GB"}

node_list = ["power3.csc.ncsu.edu",
	     "power4.csc.ncsu.edu",
    	     "power5.csc.ncsu.edu",
	     "power6.csc.ncsu.edu"]

collect_metric_list = ["cpu", "memory", "network"]
collect_metric_table = {"cpu": ["cpu_user", "cpu_system", "cpu_wio", "cpu_idle", "cpu_aidle", "cpu_nice"],
			"memory": ["mem_free", "mem_cached", "mem_buffers", "mem_shared"],
			"network": ["bytes_in", "bytes_out"]
			}

def batch(hadoop_dir, output_dir, nfs="/nfs_power2", model_list=["decoupled"]):
	for model in model_list:
		for job in job_list:


			data_dir = os.path.join(output_dir, model, "data", job)
			log_dir = os.path.join(output_dir, model, "log")
			log_file = os.path.join(log_dir, "%s.log" % job)
			execute_command("mkdir -p %s" % data_dir)
			execute_command("mkdir -p %s" % log_dir)

			intput = "dataset/%s" % job_input_table[job]
                        output = "output/%s_%s_%s" % (job, job_input_table[job], int(time.time()))

			switch_model(model=model)
                	start_hadoop(model=model)
                	sleep(60)

			time_start = int(time.time())
			submit_hadoop_job(job, intput, output, log_file=log_file, model=model, nfs=nfs)
			time_end = int(time.time())
			stop_hadoop(model=model)

			collect_data("power1.csc.ncsu.edu", time_start, time_end, data_dir)

def remote_command(host, cmd):
	remote_cmd = "ssh %s \"%s\"" % (host, cmd)
	execute_command(remote_cmd)

'''the dir path should be absolute path'''
def remote_copy(host, dir_from, dir_to):
	remote_cmd = "scp -r %s:%s/* %s" % (host, dir_from, dir_to)
	execute_command(remote_cmd)

def collect_data(ganglia_host, time_start, time_end, output_dir):
	rrds_dir = "/var/lib/ganglia/rrds"
	cluster_name = "power"
	ganglia_host = "power1.csc.ncsu.edu"
	remote_tmp_dir = os.path.join("/tmp", "rrds_%s_%s" % (cluster_name, int(time.time())))
	# how mnay seconds
	collect_step = 5
	for node in node_list:
		remote_node_dir = os.path.join(remote_tmp_dir, node)
		remote_command(ganglia_host, "mkdir -p %s" % remote_node_dir)
		for metric in collect_metric_list:
			remote_output_file = os.path.join(remote_node_dir, metric)
			rrd_list = ["DEF:%s=%s/%s/%s/%s.rrd:sum:AVERAGE XPORT:%s:'%s'" % (rrd_metric, rrds_dir, cluster_name, node, rrd_metric, rrd_metric, rrd_metric) for rrd_metric in collect_metric_table[metric]]
			rrd_list_string = " ".join(rrd_list)
			cmd = "rrdtool xport --start %s --end %s %s --step %s > %s" % (time_start, time_end, rrd_list_string, collect_step, remote_output_file)
			remote_command(ganglia_host, cmd)
	remote_copy(ganglia_host, remote_tmp_dir, output_dir)
			

def submit_hadoop_job(job, input, output, pattern="Kinmen.*", model="decoupled", log_file=None, nfs="/nfs_power2"):
	
	if "decoupled" in model:
		input = "file://%s/%s" % (nfs, input)
		output = "file://%s/%s" % (nfs, output)
	elif "reference" in model:
		input = "/%s" % (input)
                output = "/%s" % (output)

	cmd = ""
	log_file = tempfile.NamedTemporaryFile(delete=False) if log_file is None else log_file
	if job == "grep":
		cmd = "~/hadoop/bin/hadoop jar ~/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar grep -Dmapreduce.map.memory.mb=%s %s %s %s > %s 2>&1" % (map_size, input, output, pattern, log_file)
	elif job == "wordcount":
		cmd = "~/hadoop/bin/hadoop jar ~/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar wordcount -Dmapreduce.map.memory.mb=%s %s %s > %s 2>&1" % (map_size, input, output, log_file) 
	elif job == "terasort":
		cmd = "~/hadoop/bin/hadoop jar ~/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar terasort -Dmapreduce.map.memory.mb=%s %s %s > %s 2>&1" % (map_size, input, output, log_file)
	elif job == "networkintensive":
                cmd = "~/hadoop/bin/hadoop jar ~/HadoopBenchmark.jar my.oxhead.hadoop.benchmark.NetworkIntensiveJob -Dmapreduce.map.memory.mb=%s %s %s > %s 2>&1" % (map_size, input, output, log_file)

	execute_command(cmd)

def switch_model(model="decoupled"):
	print "Switch Hadoop model: %s" % model
	cmd_list = []
	for node in node_list:
		cmd = ""
		if "decoupled" in model:
			cmd = "ssh %s 'unlink ~/hadoop/conf && ln -s ~/conf_power ~/hadoop/conf'" % node
		else:
			cmd = "ssh %s 'unlink ~/hadoop/conf && ln -s ~/conf_power_reference ~/hadoop/conf'" % node
		cmd_list.append(cmd)
	execute_commands(cmd_list)

def start_hadoop(model="decoupled"):
	print "Start Hadoop model: %s" % model
	if "decoupled" not in model:
		cmd_dfs = "~/hadoop/sbin/start-dfs.sh"
		execute_command(cmd_dfs)
		sleep(1)

	cmd_yarn = "~/hadoop/sbin/start-yarn.sh"
	execute_command(cmd_yarn)

def stop_hadoop(model="decoupled"):
	print "Stop Hadoop model: %s" % model
	cmd_list = []
        if "decoupled" not in model:
                cmd_dfs = "~/hadoop/sbin/stop-dfs.sh"
                cmd_list.append(cmd_dfs)

        cmd_yarn = "~/hadoop/sbin/stop-yarn.sh"
        cmd_list.append(cmd_yarn)
	execute_commands(cmd_list)

def execute_commands(cmd_list):
	for cmd in cmd_list:
		execute_command(cmd)

def execute_command(cmd):
	print cmd
	os.system(cmd)

def main(argv):

        input_dir = ''
        output_dir = ''

        parser = argparse.ArgumentParser(description='Hadoop job submitter')

        parser.add_argument('--hadoop', required=True, help="The Hadoop directory")
	parser.add_argument('--nfs', required=True, help="The nfs directory")
        parser.add_argument("-d", '--directory', required=True, help="The output directory")
	parser.add_argument("--model", action="append", required=True, help="The Hadoop model to run")


        args = parser.parse_args()

        batch(args.hadoop, args.directory, model_list = args.model, nfs=args.nfs)

if __name__ == "__main__":
        main(sys.argv[1:])
