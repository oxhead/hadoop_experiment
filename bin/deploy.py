#!/usr/bin/python

import sys
import argparse
import env

env.init()

from my.hadoop import service
from my.hadoop import config

def main(argv):
    parser = argparse.ArgumentParser(description='Hadoop experiment')
    parser.add_argument("-d", "--directory", required=False, default="myconf", help="The output directory")
    parser.add_argument("-c", "--cluster", required=False, default="setting/cluster_config.py", help="The cluster configuration")
    args = parser.parse_args()

    cluster_config = config.get_cluster(args.cluster)
    service.deploy(cluster_config, args.directory)

if __name__ == "__main__":
    main(sys.argv[1:])
