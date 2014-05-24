#!/usr/bin/python
import logging
from my.util import command
from my.hadoop import config

logger = logging.getLogger(__name__)

# workaround for foramt
def execute(cluster, service, action, node_config_path="setting/node_config.py"):
    hadoop_dir = "~/hadoop"
    conf_dir = "%s/conf" % hadoop_dir
    dameon_script = "%s/sbin/hadoop-daemon.sh" % hadoop_dir
    yarn_script = "%s/bin/yarn" % hadoop_dir
    mapreduce_script = "%s/bin/mapred" % hadoop_dir
    hdfs_script = "%s/bin/hdfs" % hadoop_dir

    user = cluster.getUser()
    mapreduce = cluster.getMapReduceCluster()
    hdfs = cluster.getHDFSCluster()

    if service == "all":
        service_list = ["hdfs", "mapreduce", "historyserver"] if action == "start" else ["historyserver", "mapreduce", "hdfs"]
        for service in service_list:
            execute(cluster, service, action)
        return
    elif service == "mapreduce":

        if action == "start":
            # start/stop ResourceManager
            logger.info("[Service] %s ResourceManager at %s" % (action, mapreduce.getResourceManager().host))
            cmd = "%s --config %s --script %s %s resourcemanager" % (dameon_script, conf_dir, yarn_script, action)
            command.execute_remote(user, mapreduce.getResourceManager().host, cmd)
        # start/stop NodeManager
        for node in mapreduce.getNodeManagers():
            logger.info("[Service] %s NodeManager at %s" % (action, node.host))
            cmd = "%s --config %s --script %s %s nodemanager" % (dameon_script, conf_dir, yarn_script, action)
            command.execute_remote(user, node.host, cmd)
        if action == "stop":
            logger.info("[Service] %s ResourceManager at %s" % (action, mapreduce.getResourceManager().host))
            cmd = "%s --config %s --script %s %s resourcemanager" % (dameon_script, conf_dir, yarn_script, action)
            command.execute_remote(user, mapreduce.getResourceManager().host, cmd)
    elif service == "hdfs":
        if action == "format":
            logger.info("[Service] %s NameNode at %s" % (action, hdfs.getNameNode().host))
            cmd = "%s --config %s namenode -format -force" % (hdfs_script, conf_dir)
            command.execute_remote(user, hdfs.getNameNode().host, cmd)
            for node in hdfs.getDataNodes():
                logger.info("[Service] %s DataNode at %s" % (action, node.host))
                # workaround soultion, hadoop_runtime should be configurable
		node_config = config.get_node_config(node_config_path)
		data_dirs = node_config.getConfig(node.host, "hdfs.datanode.dir")
		for data_dir in data_dirs.split(","):
			data_dir = data_dir.replace("[SSD]", "").replace("[DISK]", "").strip()
			logger.info("\tClean %s" % data_dir)
			command.execute_remote(user, node.host, "rm -rf %s" % data_dir)
        else:
            if action == "start":
                logger.info("[Service] %s NameNode at %s" % (action, hdfs.getNameNode().host))
                cmd = "%s --config %s --script %s %s namenode" % (dameon_script, conf_dir, hdfs_script, action)
                # start/stop NameNode
                command.execute_remote(user, hdfs.getNameNode().host, cmd)

            # start/stop DataNode
            for node in hdfs.getDataNodes():
                logger.info("[Service] %s DataNode at %s" % (action, node.host))
                cmd = "%s --config %s --script %s %s datanode" % (dameon_script, conf_dir, hdfs_script, action)
                command.execute_remote(user, node.host, cmd)

            if action == "stop":
                logger.info("[Service] %s NameNode at %s" % (action, hdfs.getNameNode().host))
                cmd = "%s --config %s --script %s %s namenode" % (dameon_script, conf_dir, hdfs_script, action)
                command.execute_remote(user, hdfs.getNameNode().host, cmd)

    elif service == "historyserver":
        for node in mapreduce.getNodes():
            logger.info("[Service] %s HistoryServer at %s" % (action, node.host))
            cmd = "%s --config %s --script %s %s historyserver" % (dameon_script, conf_dir, mapreduce_script, action)
            command.execute_remote(user, node.host, cmd)

def deploy(cluster, conf_dir):
        for node in cluster.getNodes():
                logger.info("Deploy to %s" % node.host)
                command.execute_remote(cluster.user, node.host, "mkdir -p ~/hadoop/conf")
                # workaround for path
                command.execute("scp -r %s/%s/* %s@%s:~/hadoop/conf" % (conf_dir, node.host, cluster.user, node.host))
