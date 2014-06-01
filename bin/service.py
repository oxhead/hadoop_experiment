#!/usr/bin/python
import os
import sys
import argparse
import env

env.init()
from my.hadoop import config
from my.hadoop import service

def main(argv):
    parser = argparse.ArgumentParser(description='Hadoop experiment')
    parser.add_argument("-c", "--cluster", required=False, default="setting/cluster_config.py", help="The cluter config file")
    parser.add_argument("-a", "--action", required=True, choices=["stop", "start", "format"], help="The action")
    parser.add_argument("-s", "--service", required=True, choices=["mapreduce", "hdfs", "historyserver", "all"], help="The service compoment")
    parser.add_argument("-n", "--node", required=False, default="setting/node_config.py", help="The node config file")
    args = parser.parse_args()

    cluster = config.get_cluster(args.cluster)
    service.execute(cluster, args.service, args.action, args.node)
if __name__ == "__main__":
        main(sys.argv[1:])
