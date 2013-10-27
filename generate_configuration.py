#!/usr/bin/python

import sys
import os
import argparse
from time import sleep
import tempfile
import datetime
import time
from utils import *


configurations = {
	"io.file.buffer.size": "65536",
	"yarn.nodemanager.resource.memory-mb": "66000",
	"fs.defaultFS": "file:///nfs_power2/",
}

def update_configurations(parameter_list):
	for p in parameter_list:
		p_split = p.split("=")
		configurations[p_split[0]] = p_split[1]

def generate_configuration(conf_dir, output_dir, parameter_list):
	update_configurations(parameter_list)
	execute_command("mkdir -p %s" % output_dir)
	for f in os.listdir(conf_dir):
		file_in_path = os.path.join(conf_dir, f)
		file_out_path = os.path.join(output_dir, f)
		if "xml" in f:
			with open(file_in_path, "r") as fp_in:
				content = fp_in.read()
				for (key, value) in configurations.items():
					content = content.replace("${%s}" % key, value)
				with open(file_out_path, "w") as fp_out:
					fp_out.write(content)
		else:
			execute_command("cp %s %s" % (file_in_path, file_out_path))

def main(argv):

	parser = argparse.ArgumentParser(description='Configuration generator')

	parser.add_argument("-c", "--conf", required=True, help="The directory for the Hadoop configuration template")
	parser.add_argument("-d", "--directory", required=True, help="The output directory")
	parser.add_argument("-p", "--parameter", action="append", default=[], help="The format should be p=x")

        args = parser.parse_args()

	generate_configuration(args.conf, args.directory, args.parameter)

if __name__ == "__main__":
        main(sys.argv[1:])
