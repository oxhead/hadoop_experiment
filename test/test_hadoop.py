import os
import sys
sys.path.append('/Users/oxhead/git/hadoop_experiment')

from my.hadoop.base import *
from my.hadoop import config
from my.hadoop import service

# Test class

def test_node():
	node1 = Node("power6.csc.ncsu.edu")
	node2 = Node("power6.csc.ncsu.edu")
	x = set()
	x.add(node1)
	x.add(node2)
	assert node1 == node2
	assert len(x) == 1

# Test config

def test_get_cluster():
	cluster = config.get_cluster("../config/cluster_config.py")
	assert cluster is not None

def test_generate_configruation():
	print "[Testing] generate configuration"
	key = "xxx"
	value = 123
	additional_config = {
		key: value,
	}
	assert config.generate_config(additional_config)[key] == value

def test_generate_config_files():
	print "[Testing] generate configuration files"
	cluster_config = "../config/cluster_config.py"
	node_config = "../config/node_config.py"
	conf_dir = "../conf"
	output_dir = "../myconf"
	os.system("rm -r %s" % output_dir)
	config.generate_config_files(cluster_config, node_config, conf_dir, output_dir)

	assert len(os.listdir(output_dir)) > 0

# Test service

def test_service_deploy():
	print "[Testing] service deploy"
	cluster_config = "../config/cluster_config.py"
	conf_dir = "../myconf"

	cluster = config.get_cluster(cluster_config)
	service.deploy(cluster, conf_dir)

def test_service_action():
	print "[Testing] service action"
	cluster_config = "../config/cluster_config.py"
	cluster = config.get_cluster(cluster_config)
	conf_dir = "../myconf"

	service.execute(cluster, "all", "stop")
	service.execute(cluster, "all", "start")


def main(argv):
	test_node();
	test_get_cluster()
	test_generate_configruation()
	test_generate_config_files()
	test_service_deploy()
	test_service_action()
        
if __name__ == "__main__":
        main(sys.argv[1:])