#!/usr/bin/python

import sys
import os
import argparse
import tempfile
import datetime
import time
import mycluster
from command import *

output_file = "/tmp/dstat"

node_list = mycluster.get_node_list()

def collect_start():

	cmd = "rm -rf %s; nohup dstat -tcly -mg --vm -dr -n --tcp --float --output %s > /dev/null 2>&1 &" % (output_file, output_file)

	kill_service()
        for node in node_list:
		#print "[%s] start monitoring" % node
		remote_cmd = "ssh chsu6@%s \"%s\"" % (node, cmd)
		#print remote_cmd
		os.system(remote_cmd)

def collect_stop(output_dir):
	kill_service()	
	os.system("mkdir -p %s" % output_dir)
	for node in node_list:
		#print "[%s] stop monitoring" % node
		node_output_file = "%s/dstat_%s.csv" % (output_dir, node)
		remote_copy_cmd = "scp chsu6@%s:%s %s" % (node, output_file, node_output_file)
		Command(remote_copy_cmd).run()
	
def kill_service():
	kill_cmd = "ps aux | grep dstat | grep python | tr -s ' ' | cut -d' ' -f2 | xargs kill -9"
	for node in node_list:
                #print "[%s] kill monitoring" % node
                remote_stop_cmd = "ssh chsu6@%s \"%s\"" % (node, kill_cmd)
                #print remote_stop_cmd
                os.system(remote_stop_cmd)

def main(argv):

        parser = argparse.ArgumentParser(description='Collect data via dstat')

        #parser.add_argument("-g", '--ganglia', required=True, help="The address of the Ganglia server")
        #parser.add_argument("-c", '--cluster', required=True, help="The name of the cluster")
        #parser.add_argument("-n", '--node', action="append", required=True, help="The name of the host")
        parser.add_argument("-d", '--directory', required=True, help="The output directory")


        args = parser.parse_args()

	collect_start()
	time.sleep(10)
	collect_stop(args.directory)

if __name__ == "__main__":
        main(sys.argv[1:])
