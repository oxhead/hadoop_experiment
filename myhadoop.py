import os
import sys

def switch_configuration(configuration):
	print "Switch to configuration: %s" % configuration
       	os.system("%s service.py --user chsu6 stop all" % sys.executable)
        os.system("cp %s node_list.py" % configuration)
        os.system("%s service.py --user chsu6 format hdfs" % sys.executable)
        os.system("%s deploy.py --user chsu6" % sys.executable)
        os.system("%s service.py --user chsu6 start all" % sys.executable)

def prepare_data(size_list):
	for size in size_list:
		print "Prepare data for Terasort: %s" % size
		os.system("%s prepare_data.py -u chsu6 -t terasort -s %s" % (sys.executable, size))
		print "Prepare data for wikipedia: %s" % size
        	os.system("%s prepare_data.py -u chsu6 -t wikipedia -d /nfs_power2/dataset -s %s" % (sys.executable, size))
		print "Prepare data for kmeans: %s" % size
                os.system("%s prepare_data.py -u chsu6 -t kmeans -d /nfs_power2/dataset -s %s" % (sys.executable, size))

