#!/usr/bin/python
import os
import sys
import argparse
import env

from IPython import embed

from my.datastore.base import *

def query(db):
    init(db)
    embed()

def main(argv):
    parser = argparse.ArgumentParser(description='Hadoop experiment')
    parser.add_argument("-b", "--db", default="store/history.db", help="The output directory")
    args = parser.parse_args()

    query(args.db)

if __name__ == "__main__":
        main(sys.argv[1:])
