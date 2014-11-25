import os
import imp
import argparse
import re
import logging
import copy

from my.hadoop.base import *
from my.util import command

logger = logging.getLogger(__name__)

class NodeConfig(object):

    def __init__(self, config):
        self.config = config

    def getConfig(self, host, keyString):
	if host in self.config:
		return self.config[host][keyString]

        for (key, value) in self.config.items():
            pattern = re.compile(key)
            if pattern.match(host):
                return value[keyString]
        return None

    def getConfigPairs(self, host):
	if host in self.config:
		return self.config[host]

	for (key, value) in self.config.items():
            pattern = re.compile(key)
            if pattern.match(host):
		return value
        return None


def get_cluster(config_path):

    config = getConfigObject(config_path)

    user = config['user']

    mapreduce = MapReduceCluster()
    mapreduce.setResourceManager(Node(config['mapreduce']['ResourceManager']))
    for host in config['mapreduce']['NodeManagers']:
        mapreduce.addNodeManager(Node(host))

    hdfs = HDFSCluster()
    hdfs.setNameNode(Node(config['hdfs']['NameNode']))
    for host in config['hdfs']['DataNodes']:
        hdfs.addDataNode(Node(host))

    historyserver = HistoryServer(
        config['historyserver']['host'], config['historyserver']['port'])

    cluster = Cluster(user, mapreduce, hdfs, historyserver)

    return cluster


def get_node_config(config_path):
    config = getConfigObject(config_path)
    node_config = NodeConfig(config['config'])
    return node_config


def getConfigObject(config_path):
    config_object = {}
    execfile(config_path, config_object)
    return config_object


def generate_config(additional_config=None):
    config = None
    default_config = {
        "io.file.buffer.size": "65536",
        "fs.defaultFS": "file:///nfs_power2/",
        "yarn.nodemanager.resource.memory-mb": "66000",
        "yarn.scheduler.minimum-allocation-mb": "512",
        "yarn.scheduler.flow.assignment.model": "Flow",
        "yarn.resourcemanager.scheduler.class": "org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler",
        "yarn.inmemory.enabled": "false",
        "yarn.inmemory.prefetch.dir": "/dev/null",
        "yarn.inmemory.prefetch.window": "0",
        "yarn.inmemory.prefetch.concurrency": "0",
        "yarn.inmemory.prefetch.tasks": "0",
        "yarn.inmemory.prefetch.transfer": "false",
        "yarn.inmemory.prefetch.inputstream.enabled": "false",
        "mapreduce.job.reduces": "16",
        "mapreduce.job.reduce.slowstart.completedmaps": "0.05",
        "mapreduce.reduce.shuffle.parallelcopies": "5",
        "dfs.blocksize": "67108864",
        "dfs.replication": "3",
        "dfs.tier.enabled": "false",
        "dfs.datanode.fsdataset.volume.choosing.policy": "org.apache.hadoop.hdfs.server.datanode.fsdataset.RoundRobinVolumeChoosingPolicy",

    }
    if additional_config is not None and type(additional_config) is dict:
        config = dict(default_config.items() + additional_config.items())

    return config


def parse_config(parameter_list):
    """
    Returns
    -------
    config : dict
    """
    config = {}
    for p in parameter_list if parameter_list is not None else []:
        p_split = p.split("=")
        config[p_split[0]] = p_split[1]
    return config


def generate_config_files(cluster_config_path, node_config_path, conf_dir, output_dir, additional_config):
    """
    Generage configuration files for Hadoop.

    Parameters
    ----------
    cluster : Cluster
        The cluster info
    conf_dir : str
    output_dir : str
    parameter_list : list
            The element is in the form of "key=value"
    """

    config = generate_config(additional_config)

    cluster = get_cluster(cluster_config_path)
    mapreduce = cluster.getMapReduceCluster()
    hdfs = cluster.getHDFSCluster()

    node_config = get_node_config(node_config_path)

    # set up user name
    config['user'] = cluster.getUser()
    # set up YARN server
    config['yarn.resourcemanager.hostname'] = mapreduce.getResourceManager().host
    # set up HDFS server, the ending slash is required
    config['fs.defaultFS'] = 'hdfs://%s' % hdfs.getNameNode().host

    command.execute("mkdir -p %s" % output_dir)

    for node in cluster.getNodes():
        # create directory
        node_dir = os.path.join(output_dir, node.host)
        command.execute("mkdir -p %s" % node_dir)
        
	config_individual = copy.copy(config)
        # add node-specific configuations
        for (key, value) in node_config.getConfigPairs(node.host).items():
            config_individual[key] = value

        for f in os.listdir(conf_dir):
            file_in_path = os.path.join(conf_dir, f)
            file_out_path = os.path.join(node_dir, f)
            # node specific configuration

            if "xml" in f or "sh" in f:
                with open(file_in_path, "r") as fp_in:
                    content = fp_in.read()
                    for (key, value) in config_individual.items():
                        logger.debug("${%s} = %s", key, value)
                        content = content.replace("${%s}" % key, str(value))
                    with open(file_out_path, "w") as fp_out:
                        fp_out.write(content)
            else:
                command.execute("cp %s %s" % (file_in_path, file_out_path))


def generate_capacity_config(additional_config=None):
    config = None
    default_config = {
        "yarn.scheduler.capacity.root.queues": "default",
        "yarn.scheduler.capacity.root.capacity": "100",
        "yarn.scheduler.capacity.root.default.capacity": "100",
    }

    return default_config if additional_config is None else additional_config

    return config



def generate_capacity_scheduler_files(cluster_config_path, conf_dir, output_dir, additional_config):

    """
    Generage configuration files for Capacity Scheduler.

    Parameters
    ----------
    conf_dir : str
    output_dir : str
    additional_config : list
            The element is in the form of "key=value"
    """

    config = generate_capacity_config(additional_config)
    cluster = get_cluster(cluster_config_path)

    command.execute("mkdir -p %s" % output_dir)

    for node in cluster.getNodes():
        # create directory
        node_dir = os.path.join(output_dir, node.host)
        command.execute("mkdir -p %s" % node_dir)

        config_output_path = os.path.join(node_dir, "capacity-scheduler.xml")
        
        with open(config_output_path, "w") as fp:
            fp.write('<?xml version="1.0"?>\n')
            fp.write('<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>\n')
            fp.write('<configuration>\n')
            for (key, value) in config.iteritems():
                fp.write('\t<property>\n\t\t<name>%s</name>\n\t\t<value>%s</value>\n\t</property>\n' % (key, value))
            fp.write('</configuration>\n')


def main(argv):

    parser = argparse.ArgumentParser(description='Configuration generator')

    parser.add_argument("-c", "--conf", required=True,
                        help="The directory for the Hadoop configuration template")
    parser.add_argument(
        "-d", "--directory", required=True, help="The output directory")
    parser.add_argument("-p", "--parameter", action="append",
                        default=[], help="The format should be p=x")

    args = parser.parse_args()

    generate(args.conf, args.directory, args.parameter)

if __name__ == "__main__":
    main(sys.argv[1:])
