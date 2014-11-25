#!/usr/bin/python
import os
import sys
import argparse
import env

from my.hadoop import config
from my.hadoop import service

@env.default
def main(argv):
    parser = argparse.ArgumentParser(description='Hadoop experiment')
    parser.add_argument("-t", "--template", required=False, default="conf", help="The template configuration files")
    parser.add_argument("-d", "--directory", required=False, default="myconf", help="The output directory")
    parser.add_argument("-c", "--cluster", required=False, default="setting/cluster_config.py", help="The cluster configuration")
    parser.add_argument("-p", "--parameter", action="append", help="The configuration parameter")
    args = parser.parse_args()

    config.generate_capacity_scheduler_files(args.cluster, args.template, args.directory, config.parse_config(args.parameter))
    cluster = config.get_cluster(args.cluster)
    service.deploy(cluster, args.directory, cluster.getMapReduceCluster().getResourceManager())
    service.reload_yarn(cluster)
    

if __name__ == "__main__":
        main(sys.argv[1:])
