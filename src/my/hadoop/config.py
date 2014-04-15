import os
import imp
import argparse
import re

from my.base import *
from my.util import command

class NodeConfig(object):
        def __init__(self, config):
                self.config = config     

        def getConfig(self, host, keyString):
                for (key, value) in self.config.items():
                        pattern = re.compile(key)
                        if pattern.match(host):
                                return value[keyString]
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

        historyserver = HistoryServer(config['historyserver']['host'], config['historyserver']['port'])

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
                "yarn.nodemanager.resource.memory-mb": "66000",
                "fs.defaultFS": "file:///nfs_power2/",
                "mapreduce.job.reduces": "16",
                "yarn.scheduler.minimum-allocation-mb": "512",
                "yarn.scheduler.flow.assignment.model": "Flow",
                "mapreduce.job.reduce.slowstart.completedmaps": "0.05",
                "mapreduce.reduce.shuffle.parallelcopies": "5",
                "dfs.replication": "3",
                "yarn.resourcemanager.scheduler.class": "org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler",
                "yarn.inmemory.enabled": "false",
                "yarn.inmemory.prefetch.dir": "/dev/null",
                "yarn.inmemory.prefetch.window": "0",
                "yarn.inmemory.prefetch.concurrency": "0",
                "yarn.inmemory.prefetch.tasks": "0",
                "yarn.inmemory.prefetch.transfer": "false",
                "yarn.inmemory.prefetch.inputstream.enabled": "false",

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
        for p in parameter_list:
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


        # is better to use /home or ~
        config['hadoop.runtime.dir'] = "/home/%s/hadoop_runtime" % cluster.getUser()
        # set up YARN server
        config['yarn.resourcemanager.hostname'] = mapreduce.getResourceManager().host
        # set up HDFS server, the ending slash is required
        config['fs.defaultFS'] = 'hdfs://%s' % hdfs.getNameNode().host

        command.execute("mkdir -p %s" % output_dir)

        for node in cluster.getNodes():
                # create directory
                node_dir = os.path.join(output_dir, node.host)
                command.execute("mkdir -p %s" % node_dir)

                for f in os.listdir(conf_dir):
                        file_in_path = os.path.join(conf_dir, f)
                        file_out_path = os.path.join(node_dir, f)
                        # node specific configuration
                        config['yarn.nodemanager.resource.memory-mb'] = node_config.getConfig(node.host, "memory")
                        config['yarn.scheduler.minimum-allocation-mb'] =  node_config.getConfig(node.host, "slot_size")
                        config['JAVA_HOME'] = node_config.getConfig(node.host, "jdk_dir")
                        if "xml" in f or "sh" in f:
                                with open(file_in_path, "r") as fp_in:
                                        content = fp_in.read()
                                        for (key, value) in config.items():
                                                content = content.replace("${%s}" % key, value)
                                        with open(file_out_path, "w") as fp_out:
                                                fp_out.write(content)
                        else:
                                command.execute("cp %s %s" % (file_in_path, file_out_path))


def main(argv):

        parser = argparse.ArgumentParser(description='Configuration generator')

        parser.add_argument("-c", "--conf", required=True, help="The directory for the Hadoop configuration template")
        parser.add_argument("-d", "--directory", required=True, help="The output directory")
        parser.add_argument("-p", "--parameter", action="append", default=[], help="The format should be p=x")

        args = parser.parse_args()

        generate(args.conf, args.directory, args.parameter)

if __name__ == "__main__":
        main(sys.argv[1:])