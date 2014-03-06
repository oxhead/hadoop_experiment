import node_list as data
from topology import *

def load():
        cluster_mapreduce = MapReduceCluster()
        cluster_mapreduce.setResourceManager(Node(data.mapreduce['ResourceManager']))
        for host in data.mapreduce['NodeManagers']:
                cluster_mapreduce.addNodeManager(Node(host))

        cluster_hdfs = HDFSCluster()
        cluster_hdfs.setNameNode(Node(data.hdfs['NameNode']))
        for host in data.hdfs['DataNodes']:
                cluster_hdfs.addDataNode(Node(host))
        cluster = Cluster()

	cluster_ganglia = Node(data.ganglia)

        cluster.mapreduce = cluster_mapreduce
        cluster.hdfs = cluster_hdfs
	cluster.ganglia = cluster_ganglia
        return cluster

def get_node_list():
	cluster = load()
	node_list = []
	node_list.append(cluster.mapreduce.getResourceManager().host)
	for node in cluster.mapreduce.getNodeManagers():
		node_list.append(node.host)
	node_list.append(cluster.hdfs.getNameNode().host)
	for node in cluster.hdfs.getDataNodes():
		node_list.append(node.host)
	return set(node_list)

