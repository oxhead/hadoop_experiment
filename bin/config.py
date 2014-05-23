#!/usr/bin/python

import sys
import argparse
import env

env.init()

from my.hadoop import config

def main(argv):
    parser = argparse.ArgumentParser(description='Hadoop experiment')
    parser.add_argument("-t", "--template", required=False, default="conf", help="The template configuration files")
    parser.add_argument("-d", "--directory", required=False, default="myconf", help="The output directory")
    parser.add_argument("-c", "--cluster", required=False, default="setting/cluster_config.py", help="The cluster configuration")
    parser.add_argument("-n", "--node", required=False, default="setting/node_config.py", help="The node configuration")
    parser.add_argument("-p", "--parameter", action="append", help="The configuration parameter")
    args = parser.parse_args()

    config.generate_config_files(args.cluster, args.node, args.template, args.directory, config.parse_config(args.parameter))


if __name__ == "__main__":
    main(sys.argv[1:])
