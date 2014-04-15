class Node(object):
        def __init__(self, host):
                self.host = host
        def getHost(self):
                return self.host
        def __eq__(self, other):
                return (isinstance(other, self.__class__) and self.host == other.host)

        def __ne__(self, other):
                return not self.__eq__(other)
        def __repr__(self):
                return self.host
        def __hash__(self):
                return hash(self.__repr__())


class Cluster(object):
        def __init__(self, user, mapreduce, hdfs, history_server):
                self.user = user
                self.mapreduce = mapreduce
                self.hdfs = hdfs
                self.history_server = history_server
                
        def getUser(self):
                return self.user

        def setMapReduceCluster(self, cluster):
                self.mapreduce = clsuter

        def setHDFSCluster(self, cluster):
                self.hdfs = cluster

        def getMapReduceCluster(self):
                return self.mapreduce

        def getHDFSCluster(self):
                return self.hdfs

        def getHistoryServer(self):
                return self.history_server

        def getNodes(self):
                node_list = []
                node_list.extend(self.getMapReduceCluster().getNodes())
                node_list.extend(self.getHDFSCluster().getNodes())
                return list(set(node_list))


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

        def getNodes(self):
                node_list = []
                node_list.append(self.getResourceManager())
                node_list.extend(self.getNodeManagers())
                return list(set(node_list))

class HDFSCluster(object):
        def __init__(self):
                # NamNode
                self.nn = None
                # List of DataNodes
                self.dns = []

        def setNameNode(self, node):
                self.nn = node

        def addDataNode(self, node):
                self.dns.append(node)

        def getNameNode(self):
                return self.nn

        def getDataNodes(self):
                return self.dns

        def getNodes(self):
                node_list = []
                node_list.append(self.getNameNode())
                node_list.extend(self.getDataNodes())
                return list(set(node_list))

class HistoryServer(object):
        def __init__(self, host, port):
                self.host = host
                self.port = port