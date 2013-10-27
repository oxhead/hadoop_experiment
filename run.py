#!/usr/bin/python

import os
import sys
import time
import argparse
from base import *

def run():
		
	

def main(argv):
	
	parser = argparse.ArgumentParser(description='Hadoop job submitter')

        parser.add_argument('--hadoop', required=True, help="The Hadoop directory")
        parser.add_argument('--nfs', required=True, help="The nfs directory")
        parser.add_argument("-d", '--directory', required=True, help="The output directory")
        parser.add_argument("--model", action="append", required=True, help="The Hadoop model to run")
        parser.add_argument("--job", action="append", required=True, help="The Hadoop jobs to run")


        args = parser.parse_args()

        run(args.hadoop, args.directory, model_list = args.model, nfs=args.nfs, job_list = args.job)

if __name__ == "__main__":
        main(sys.argv[1:])
