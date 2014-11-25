#!/usr/bin/python
import logging
import os
import time
from my.util import command
from my.hadoop import config
from my.hadoop.base import Node

logger = logging.getLogger(__name__)

# workaround for foramt
def execute(cluster, service, action, node_config_path="setting/node_config.py"):
    hadoop_dir = "~/hadoop"
    conf_dir = "%s/conf" % hadoop_dir
    dameon_script = "%s/sbin/hadoop-daemon.sh" % hadoop_dir
    yarn_dameon_script = "%s/sbin/yarn-daemon.sh" % hadoop_dir
    yarn_script = "%s/bin/yarn" % hadoop_dir
    mapreduce_script = "%s/bin/mapred" % hadoop_dir
    hdfs_script = "%s/bin/hdfs" % hadoop_dir

    user = cluster.getUser()
    mapreduce = cluster.getMapReduceCluster()
    hdfs = cluster.getHDFSCluster()
    historyserver = cluster.getHistoryServer()

    node_config = config.get_node_config(node_config_path)

    returncode_list = []

    if service == "all":
        service_list = ["hdfs", "mapreduce", "historyserver"] if action == "start" else ["historyserver", "mapreduce", "hdfs"]
        for service in service_list:
            execute(cluster, service, action, node_config_path)
        return
    elif service == "mapreduce":
        if action == "format":
            for node in hdfs.getDataNodes():
                logger.info("[Service] %s NodeManager at %s" % (action, node.host))
                # workaround soultion, hadoop_runtime should be configurable
                hadoop_dirs = node_config.getConfig(node.host, "yarn.nodemanager.local-dirs")
                for hadoop_dir in hadoop_dirs.split(","):
                        hadoop_dir = hadoop_dir.strip()
                        logger.info("\tClean %s" % hadoop_dir)
                        command.execute_remote(user, node.host, "rm -rf %s/*" % hadoop_dir)
        else:
            if action == "start":
                # start/stop ResourceManager
                logger.info("[Service] %s ResourceManager at %s" % (action, mapreduce.getResourceManager().host))
                cmd = "%s --config %s %s resourcemanager" % (yarn_dameon_script, conf_dir, action)
                command.execute_remote(user, mapreduce.getResourceManager().host, cmd)
            # start/stop NodeManager
            for node in mapreduce.getNodeManagers():
                logger.info("[Service] %s NodeManager at %s" % (action, node.host))
                cmd = "%s --config %s %s nodemanager" % (yarn_dameon_script, conf_dir, action)
                command.execute_remote(user, node.host, cmd)
            if action == "stop":
                logger.info("[Service] %s ResourceManager at %s" % (action, mapreduce.getResourceManager().host))
                cmd = "%s --config %s %s resourcemanager" % (yarn_dameon_script, conf_dir, action)
                command.execute_remote(user, mapreduce.getResourceManager().host, cmd)
    elif service == "hdfs":
        if action == "format":
            logger.info("[Service] %s NameNode at %s" % (action, hdfs.getNameNode().host))
            cmd = "%s --config %s namenode -format -force" % (hdfs_script, conf_dir)
            command.execute_remote(user, hdfs.getNameNode().host, cmd)
            for node in hdfs.getDataNodes():
                logger.info("[Service] %s DataNode at %s" % (action, node.host))
                # workaround soultion, hadoop_runtime should be configurable
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
        logger.info("[Service] %s HistoryServer at %s" % (action, historyserver.host))
        cmd = "%s --config %s --script %s %s historyserver" % (dameon_script, conf_dir, mapreduce_script, action)
        command.execute_remote(user, historyserver.host, cmd)

def node_action(host, role, action, node_config_path="setting/node_config.py", user='root'):
    '''
    execute action (start/stop/kill) on a given node

    Args:
        node: hostname of the node
        role: the role on the given node
        action: to start, stop or kill
        node_config_path: config path
    '''

    node = Node(host)
    hadoop_dir = "~/hadoop"
    conf_dir = "%s/conf" % hadoop_dir
    dameon_script = "%s/sbin/hadoop-daemon.sh" % hadoop_dir
    yarn_dameon_script = "%s/sbin/yarn-daemon.sh" % hadoop_dir
    yarn_script = "%s/bin/yarn" % hadoop_dir
    mapreduce_script = "%s/bin/mapred" % hadoop_dir
    hdfs_script = "%s/bin/hdfs" % hadoop_dir

    if role == "resourcemanager":
        logger.info("[Service] %s ResourceManager at %s" % (action, node.host))
        cmd = "%s --config %s %s resourcemanager" % (yarn_dameon_script, conf_dir, action)
        command.execute_remote(user, node.host, cmd)
    elif role == "nodemanager":
        logger.info("[Service] %s NodeManager at %s" % (action, node.host))
        if action == 'start' or action == 'stop':
            cmd = "%s --config %s %s nodemanager" % (yarn_dameon_script, conf_dir, action)
            command.execute_remote(user, node.host, cmd)
        elif action == 'kill':
            cmd = "pkill -f 'org.apache.*.NodeManager'"
            command.execute_remote(user, node.host, cmd)
        elif action == 'reboot':
            cmd = 'sudo reboot -f'
            command.execute_remote(user, node.host, cmd)
        elif action == 'panic':
            cmd = 'sudo bash -c "echo c > /proc/sysrq-trigger"'
            command.execute_remote(user, node.host, cmd)
            
    elif role == "namenode":
        logger.info("[Service] %s NameNode at %s" % (action, node.host))
        cmd = "%s --config %s --script %s %s namenode" % (dameon_script, conf_dir, hdfs_script, action)
        # start/stop NameNode
        command.execute_remote(user, node.host, cmd)
    elif role == "datanode":
        logger.info("[Service] %s DataNode at %s" % (action, node.host))
        cmd = "%s --config %s --script %s %s datanode" % (dameon_script, conf_dir, hdfs_script, action)
        command.execute_remote(user, node.host, cmd)
    
def deploy(cluster, conf_dir, included_node=None):
        for node in cluster.getNodes():
                if included_node is None or node.host == included_node.host: 
                    logger.info("Deploy to %s" % node.host)
                    command.execute_remote(cluster.user, node.host, "mkdir -p ~/hadoop/conf")
                    # workaround for path
                    command.execute("scp -r %s/%s/* %s@%s:~/hadoop/conf" % (conf_dir, node.host, cluster.user, node.host))

def reload_yarn(cluster):
    hadoop_dir = "~/hadoop"
    conf_dir = "%s/conf" % hadoop_dir
    yarn_script = "%s/bin/yarn" % hadoop_dir

    user = cluster.getUser()
    mapreduce = cluster.getMapReduceCluster()

    logger.info("[Service] Reload ResourceManager at %s" % (mapreduce.getResourceManager().host))
    cmd = "%s --config %s rmadmin -refreshQueues" % (yarn_script, conf_dir)
    command.execute_remote(user, mapreduce.getResourceManager().host, cmd)
    

def get_tmp_file():
	return "/tmp/service_%s.returncode" % int(time.time())
