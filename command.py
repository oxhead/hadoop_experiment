#!/usr/bin/python

import sys
import os


class Command(object):
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


def execute_remote_commnad(node, cmd):
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
	c = Command(cmd)
	c.run()
	print c.output
	print c.error
	return c.output
