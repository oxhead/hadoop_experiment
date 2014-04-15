#!/usr/bin/python
import os
LIB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), "src")

import sys
sys.path.append(LIB_PATH)

import time

from my.experiment.reporttool import *
from my.experiment.decorator import *


@ReportGenerator("wiating_time")
def test_report(n):
	print "[Testing] creat job"


def main(argv):
	test_report(1)

if __name__ == "__main__":
        main(sys.argv[1:])
