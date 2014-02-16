class Node(object):
	def __init__(self, host):
		self.host = host
	def getHost(self):
		return self.host

class Cluster(object):
	def __init__(self):
		pass
	def setMapReduceCluster(cluster):
		self.mapreduce = clsuter
	def setHDFSCluster(cluster):
		self.hdfs = cluster

class MapReduceCluster(object):
	def __init__(self):
		self.rm = None
		self.nms = []

	def setResourceManager(self, node):
		self.rm = node

	def addNodeManager(self, node):
		self.nms.append(node)

	def getResourceManager(self):
		return self.rm

	def getNodeManagers(self):
		return self.nms

class HDFSCluster(object):
	def __init__(self):
		self.nn = None
		self.dns = []

	def setNameNode(self, node):
		self.nn = node

	def addDataNode(self, node):
		self.dns.append(node)

	def getNameNode(self):
		return self.nn

	def getDataNodes(self):
		return self.dns
