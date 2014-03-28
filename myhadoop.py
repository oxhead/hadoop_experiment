import os
import sys
from command import *

def switch_configuration(configuration, scheduler="fifo"):
	print "Switch to configuration: %s" % configuration
	print "Status: stop Hadoop service"
       	Command("%s service.py --user chsu6 stop all" % sys.executable).run()
	print "Status: copy configuration files"
        os.system("cp %s node_list.py" % configuration)
	print "Status: deploy Hadoop service"
	(scheduler_class, scheduler_parameter) = lookup_scheduler(scheduler)
	Command("%s deploy.py --user chsu6 -p yarn.resourcemanager.scheduler.class=%s -p %s" % (sys.executable, scheduler_class, scheduler_parameter)).run()
	print "Status: format HDFS storage"
        Command("%s service.py --user chsu6 format hdfs" % sys.executable).run()
	print "Status: start Hadoop service"
        Command("%s service.py --user chsu6 start all" % sys.executable).run()
	print "Status: completed configuring Hadoop"

def prepare_data(size_list):
	for size in size_list:
		print "Prepare data for Terasort: %s" % size
		Command("%s prepare_data.py -u chsu6 -t terasort -s %s" % (sys.executable, size)).run()
		print "Prepare data for wikipedia: %s" % size
        	Command("%s prepare_data.py -u chsu6 -t wikipedia -d /nfs_power2/dataset -s %s" % (sys.executable, size)).run()
		#print "Prepare data for kmeans: %s" % size
                #Command("%s prepare_data.py -u chsu6 -t kmeans -d /nfs_power2/dataset -s %s" % (sys.executable, size)).run()

def lookup_scheduler(scheduler):
	scheduler_class = "org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler"
	scheduler_parameter = "yarn.scheduler.flow.assignment.model=Flow"
	if scheduler.lower() == "fifo":
		scheduler_class = "org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler"
	elif scheduler.lower() == "fair":
		scheduler_class = "org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler"
	elif scheduler.lower() == "capacity":
		scheduler_class = "org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler"
	elif scheduler.lower() == "flow":
		scheduler_class = "org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow.FlowScheduler"
		scheduler_parameter = "yarn.scheduler.flow.assignment.model=Flow"
	elif scheduler.lower() == "balancing":
                scheduler_class = "org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow.FlowScheduler"
                scheduler_parameter = "yarn.scheduler.flow.assignment.model=Balancing"
	elif scheduler.lower() == "color":
		scheduler_class = "org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow.FlowScheduler"
		scheduler_parameter = "yarn.scheduler.flow.assignment.model=Color"
	return (scheduler_class, scheduler_parameter)
