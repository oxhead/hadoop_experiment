#!/usr/bin/python
import os
import sys
import argparse
import env

from my.hadoop import service

@env.default
def main(argv):
    parser = argparse.ArgumentParser(description='Hadoop experiment')
    parser.add_argument("-n", "--node", required=True, help="The host name of the node")
    parser.add_argument("-r", "--role", required=True, choices=["resourcemanager", "nodemanager", "namenode", "datanode"])
    parser.add_argument("-a", "--action", required=True, choices=["stop", "start", "kill", "reboot", "panic"], help="The action")
    args = parser.parse_args()

    service.node_action(args.node, args.role, args.action)

if __name__ == "__main__":
        main(sys.argv[1:])
