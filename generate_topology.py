#!/usr/bin/python

import sys
import os
import argparse
from topology import *

def generate(mapreduce, hdfs):
	cluster_mapreduce = MapReduceCluster()
	cluster_mapreduce.setResourceManager(Node(mapreduce['ResourceManager']))
	for host in mapreduce['NodeManagers']:
		cluster_mapreduce.addNodeManager(Node(host))
	cluster_hdfs = HDFSCluster()
	cluster_hdfs.setNameNode(Node(hdfs['NameNode']))
	for host in hdfs['DataNodes']:
		cluster_hdfs.addDataNode(Node(host))
	cluster = Cluster()
	cluster.mapreduce = cluster_mapreduce
	cluster.hdfs = cluster_hdfs
	return cluster

def main(argv):
	import load_data
        mapreduce = load_data.getMapReduceClusterConfig()
        hdfs = load_data.getHDFSClusterConfig()
	cluster = generate(mapreduce, hdfs)
	print cluster.mapreduce.rm.host

if __name__ == "__main__":
        main(sys.argv[1:])
