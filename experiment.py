#!/usr/bin/python

import sys
import os
import argparse
from time import sleep
import tempfile
import datetime
import time

class Experiment(object):
    def __init__(self):
        self.command = command
    def run(self, shell=True):
        import subprocess as sp
        process = sp.Popen(self.command, shell = shell, stdout = sp.PIPE, stderr = sp.PIPE)
        self.pid = process.pid
        self.output, self.error = process.communicate()
        self.failed = process.returncode
        return self
    @property
    def returncode(self):
        return self.failed

def main(argv):

        input_dir = ''
        output_dir = ''

        parser = argparse.ArgumentParser(description='Run your customized experiment here')

        parser.add_argument('--hadoop', required=True, help="The Hadoop directory")
	parser.add_argument('--nfs', required=True, help="The nfs directory")
        parser.add_argument("-d", '--directory', required=True, help="The output directory")
	parser.add_argument("--model", action="append", required=True, help="The Hadoop model to run")


        args = parser.parse_args()

        batch(args.hadoop, args.directory, model_list = args.model, nfs=args.nfs)

if __name__ == "__main__":
        main(sys.argv[1:])
