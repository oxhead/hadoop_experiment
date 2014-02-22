#!/usr/bin/python

import sys
import os
import subprocess
from subprocess import call

class Command(object):
	def __init__(self, command, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE):
        	self.command = command
        	self.shell = shell
		self.stdin = stdin
		self.stdout = stdout
		self.stderr = stderr

	def run(self, async=False):
		self.process = subprocess.Popen(self.command, shell=self.shell, stdout=self.stdout, stderr=self.stderr)
        	self.pid = self.process.pid
        	self.output, self.error = self.process.communicate()
		if not async:
			self.process.wait()
        	return self

    	def wait(self):
		self.process.wait()

    	@property
    	def returncode(self):
        	return self.process.returncode

def execute_remote_command(node, cmd):
        remote_cmd = "ssh %s@%s \"%s\"" % (node.user, node.host, cmd)
        execute_command(remote_cmd)	

def execute_remote_commands(node, cmds):
	for cmd in cmds:
		remote_command(node, cmd)

def execute_commands(cmd_list):
        for cmd in cmd_list:
                execute_command(cmd)

def execute_command(cmd):
        #os.system(cmd)
	#c = Command(cmd)
	#c.run()
	#print c.output
	#print c.error
	#return c.output
	print cmd
	retcode = call(cmd, shell=True)
	return retcode

def execute_command_in_background(cmd):
	print cmd
	os.system("%s &" % cmd)
