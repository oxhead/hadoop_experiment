#!/usr/bin/python
import os
import sys
import argparse
import env

env.init()
from my.hadoop import configtool
from my.hadoop import service
from my.hadoop.base import HadoopSetting

def main(argv):
    parser = argparse.ArgumentParser(description='Hadoop experiment')
    parser.add_argument("-c", "--cluster", required=False, default="setting/cluster_config.py", help="The cluter config file")
    parser.add_argument("-n", "--node", required=False, default="setting/node_config.py", help="The node config file")
    parser.add_argument("-a", "--action", required=True, choices=["stop", "start", "format"], help="The action")
    parser.add_argument("-s", "--service", required=True, choices=["mapreduce", "hdfs", "historyserver", "all"], help="The service compoment")
    args = parser.parse_args()

    cluster_config = configtool.parse_cluster_config(args.cluster)
    node_config = configtool.parse_node_config(args.node)
    setting = HadoopSetting(cluster_config, node_config)
    service.execute(setting, args.service, args.action)
if __name__ == "__main__":
        main(sys.argv[1:])
