#!/usr/bin/python

from node_list import mapreduce, hdfs
from node_configuration import memory, slot_size, jdk_dir

def getMapReduceClusterConfig():
	return mapreduce
def getHDFSClusterConfig():
	return hdfs
def getMemoryConfig():
	return memory
def getSlotSizeConfig():
	return slot_size
def getJDKDirConfi():
	return jdk_dir

def main(argv):
	pass

if __name__ == "__main__":
        main(sys.argv[1:])
