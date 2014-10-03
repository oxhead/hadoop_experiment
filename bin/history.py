#!/usr/bin/python
import os
import sys
import argparse
import env

env.init()
from my.experiment import historytool
from my.hadoop import config

def main(argv):
    parser = argparse.ArgumentParser(description='Hadoop experiment')
    parser.add_argument("-c", "--cluster", required=False, default="setting/cluster_config.py", help="The cluter config file")
    parser.add_argument("-n", "--node", required=False, default="setting/node_config.py", help="The node config file")
    args = parser.parse_args()

    cluster = config.get_cluster(args.cluster)
    historytool.dump(cluster.getHistoryServer(), 'json.json')

if __name__ == "__main__":
        main(sys.argv[1:])
