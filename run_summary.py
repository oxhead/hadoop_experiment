#!/usr/bin/python

import os
import sys
import time
import argparse

'''from http://www.daniweb.com/software-development/python/threads/281000/using-the-bash-output-in-python'''
class Command(object):
    """Run a command and capture it's output string, error string and exit status"""
    def __init__(self, command):
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


def show_summary(output_dir):
	log_dir = os.path.join(output_dir, "log")
	log_files = [os.path.join(log_dir, f) for f in os.listdir(log_dir) if os.path.isfile(os.path.join(log_dir, f))]
	for f in log_files:
		#print f
		cmd_time_start = Command("bash -c \"grep 'map 0%% reduce 0%%' %s | head -n 1 | cut -d' ' -f2\"" % f)
		cmd_time_end = Command("bash -c \"grep 'map 100%% reduce 100%%' %s | head -n 1 | cut -d' ' -f2\"" % f)
		#print cmd_time_start.command
		cmd_time_start.run()
		cmd_time_end.run()
		time_start_output = cmd_time_start.output.rstrip()
		time_end_output =cmd_time_end.output.rstrip()
		#print time_start_output, time_end_output
		time_start = time.strptime(time_start_output, "%H:%M:%S")
		time_end = time.strptime(time_end_output, "%H:%M:%S")
		time_delta = time.mktime(time_end) - time.mktime(time_start)
		print "%s: %s (%s, %s)" % (f, time_delta, time_start_output, time_end_output)
			

def main(argv):

        input_dir = ''
        output_dir = ''

        parser = argparse.ArgumentParser(description='Convert RRDTool output to csv')

        parser.add_argument("-d", '--directory', required=True, help="The experiment run directory")

        args = parser.parse_args()

        show_summary(args.directory)

if __name__ == "__main__":
        main(sys.argv[1:])
