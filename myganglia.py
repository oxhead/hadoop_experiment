#!/usr/bin/python

import sys
import os
import argparse
import tempfile
import datetime
import time
import mycluster
from command import *

metric_list = [
        "cpu_report", "network_report",
        "cpu_user", "cpu_system", "cpu_wio", "cpu_idle", "cpu_aidle", "cpu_nice",
        "mem_free", "mem_cached", "mem_buffers", "mem_shared",
        "bytes_in", "bytes_out",
]
collect_metric_list = ["cpu", "memory", "network"]
collect_metric_table = {"cpu": ["cpu_user", "cpu_system", "cpu_wio", "cpu_idle", "cpu_aidle", "cpu_nice"],
                        "memory": ["mem_free", "mem_cached", "mem_buffers", "mem_shared"],
                        "network": ["bytes_in", "bytes_out"]
                        }

                        #@TODO: add time range
def collect(output_dir, time_start=None, time_end=None):

        cluster = mycluster.load()
        ganglia_server = cluster.ganglia.host

        collect_time = datetime.datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
        export_dir = os.path.join(output_dir, collect_time)
        os.system("mkdir -p %s" % export_dir)

        node_list = []
        node_list.append(cluster.mapreduce.getResourceManager())
        node_list.extend(cluster.mapreduce.getNodeManagers())
        node_list.append(cluster.hdfs.getNameNode())
        node_list.extend(cluster.hdfs.getDataNodes())

        for node in node_list:
                node_cluster = lookup_cluster(node.host)
                node_host = node.host
                for metric in metric_list:
                        export_file = os.path.join(export_dir, "%s_%s.csv" % (node_host, metric))
                        if time_start is None or time_end is None:
                                cmd = "curl --silent 'http://%s/ganglia2/graph.php?c=%s&h=%s&n=1&v=&m=%s&csv=1&step=1&r=hour' > %s" % (ganglia_server, node_cluster, node_host,metric, export_file)
				print cmd
                                os.system(cmd)
                        else:
                                i = 2
                                for i in range(2):
                                        export_file_real = os.path.join(export_dir, "%s_%s_%s.csv" % (node_host, metric, str(i)))
                                        time_start_real = time_start - 60*60*i
                                        time_end_real = time_end - 60*60*i
                                        cmd = "curl --silent 'http://%s/ganglia2/graph.php?c=%s&h=%s&n=1&v=&m=%s&cs=%s&ce=%s&csv=1&step=1' > %s" % (ganglia_server, node_cluster, node_host, metric, str(time_start_real), str(time_end_real), export_file_real)

def collect_native(output_dir, time_start=None, time_end=None):
        rrds_dir = "/var/lib/ganglia/rrds"
        cluster_name = "power"
        ganglia_host = "power1.csc.ncsu.edu"
        remote_tmp_dir = os.path.join("/tmp", "rrds_%s_%s" % (cluster_name, int(time.time())))
        # how mnay seconds
        collect_step = 5

        cluster = mycluster.load()
        ganglia_server = cluster.ganglia.host

        collect_time = datetime.datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
        export_dir = os.path.join(output_dir, collect_time)
        os.system("mkdir -p %s" % export_dir)

        node_list = []
        node_list.append(cluster.mapreduce.getResourceManager())
        node_list.extend(cluster.mapreduce.getNodeManagers())
        node_list.append(cluster.hdfs.getNameNode())
        node_list.extend(cluster.hdfs.getDataNodes())

        for node in node_list:
                node_cluster = lookup_cluster(node.host)
                node_host = node.host

                remote_node_dir = os.path.join(remote_tmp_dir, node_host)
                execute_remote_command_nouser(ganglia_server, "mkdir -p %s" % remote_node_dir)
                for metric in collect_metric_list:
                        remote_output_file = os.path.join(remote_node_dir, metric)
                        rrd_list = ["DEF:%s=%s/%s/%s/%s.rrd:sum:AVERAGE XPORT:%s:'%s'" % (rrd_metric, rrds_dir, node_cluster, node_host, rrd_metric, rrd_metric, rrd_metric) for rrd_metric in collect_metric_table[metric]]
                        rrd_list_string = " ".join(rrd_list)
                        if time_start is None or time_end is None:
                                cmd = "rrdtool xport %s --step %s > %s" % (rrd_list_string, collect_step, remote_output_file)
                        else:
                                cmd = "rrdtool xport --start %s --end %s %s --step %s > %s" % (time_start, time_end, rrd_list_string, collect_step, remote_output_file)
                        execute_remote_command_nouser(ganglia_server, cmd)
        remote_copy(ganglia_server, remote_tmp_dir, output_dir)


def lookup_cluster(host):
        if "power" in host:
		return "power"
	elif "bc1" in host:
		return "bc1"

def main(argv):

        input_dir = ''
        output_dir = ''

        parser = argparse.ArgumentParser(description='Collect data from Ganglia web 2')

        #parser.add_argument("-g", '--ganglia', required=True, help="The address of the Ganglia server")
        #parser.add_argument("-c", '--cluster', required=True, help="The name of the cluster")
        #parser.add_argument("-n", '--node', action="append", required=True, help="The name of the host")
        #parser.add_argument("-d", '--directory', required=True, help="The output directory")


        #args = parser.parse_args()

        #collect(args.ganglia, args.cluster, args.node, args.directory)
        collect("download")
        #collect_native("download")

if __name__ == "__main__":
        main(sys.argv[1:])
