import re

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
        def isNodeManager(self, node):
                return any(x == node for x in self.getMapReduceCluster().getNodeManagers())
        def isDataNode(self, node):
                return any(x == node for x in self.getHDFSCluster().getDataNodes())
        def isResourceManager(self, node):
                return node == self.getMapReduceCluster().getResourceManager()
        def isNameNode(self, node):
                return node == self.getHDFSCluster().getNameNode()


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

class HadoopSetting():
    ''' Class for Hadoop related setting

    Define a Hadoop cluster class to store all configurations in one place.

    Attributes:
        cluster (dict): the cluster topology
        config (NodeConfig): the configuration object
        parameters (dict): the additional parameters
        conf_dir (str): the path to Hadoop configuration template
    '''

    def __init__(self, cluster, config, parameters={}, conf_dir="conf"):
        self.cluster = cluster
        self.config = config
        self.parameters = parameters
        self.conf_dir = conf_dir
        # special configurations
        # set up user name
        self.parameters['user'] = self.cluster.getUser()
        # set up YARN server
        self.parameters['yarn.resourcemanager.hostname'] = self.cluster.getMapReduceCluster().getResourceManager().host
        # set up HDFS server, the ending slash is required
        self.parameters['fs.defaultFS'] = 'hdfs://%s' % self.cluster.getHDFSCluster().getNameNode().host

    def add_parameters(self, parameters):
        self.parameters.update(parameters)

    def get_config(self, host, key):
        value = self.config.get_config(host, key)
        if value is None:
            value = self.parameters[key] if key in self.parameters else None
        return value

class NodeConfig(object):
    '''A configuration object

    This object supports basic regular expression.  Given a hostname key and a configuration key, it returns a value.
    '''

    def __init__(self, config):
        self.config = config

    def get_config(self, host, keyString):
        if host in self.config:
                return self.config[host][keyString]

        for (key, value) in self.config.items():
            pattern = re.compile(key)
            if pattern.match(host):
                return value[keyString]
        return None

    def get_config_pairs(self, host):
        if host in self.config:
                return self.config[host]

        for (key, value) in self.config.items():
            pattern = re.compile(key)
            if pattern.match(host):
                return value
        return None
