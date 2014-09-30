import sys
import os
import subprocess
import logging
from subprocess import call

logger = logging.getLogger(__name__)

class Command(object):

        def __init__(self, command, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE):
                self.command = command
                self.shell = shell
                self.stdin = stdin
                self.stdout = stdout
                self.stderr = stderr

        def run(self, async=False):
                self.process = subprocess.Popen(
                    self.command, shell=self.shell, stdout=self.stdout, stderr=self.stderr)
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


def execute(cmd, output=False):
    logger.debug(cmd)
    retcode = -1
    if output:
        retcode = os.system("%s" % cmd)
    else:
        retcode = os.system("%s > /dev/null 2>&1" % cmd)
    logger.debug("execution result: %s", retcode)
    return retcode


def execute_async(cmd, output=False):
    logger.debug(cmd)
    retcode = -1
    if output:
        retcode = os.system("(%s) &" % cmd)
    else:
        retcode = os.system("(%s > /dev/null 2>&1) &" % cmd)
    return retcode


def execute_remote(user, host, cmd, output=False):
    remote_cmd = "ssh -t %s@%s \"%s\"" % (user, host, cmd)
    execute(remote_cmd, output)
