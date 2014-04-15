import os
import time
import datetime

from my.experiment.base import *

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
        scheduler_parameter = "Flow"
    elif scheduler.lower() == "balancing":
        scheduler_class = "org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow.FlowScheduler"
        scheduler_parameter = "Balancing"
    elif scheduler.lower() == "color":
        scheduler_class = "org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow.FlowScheduler"
        scheduler_parameter = "Color"
    elif scheduler.lower() == "colorstorage":
        scheduler_class = "org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow.FlowScheduler"
        scheduler_parameter = "ColorStorage"
    return (scheduler_class, scheduler_parameter)


def lookup_size_name(job_size):
    if job_size < 1024:
        return "%sMB" % job_size
    elif job_size % 1024 == 0:
        return "%sGB" % (job_size / 1024)
    else:
        return None


def convert_unit(size):
        if "MB" in size:
                return int(size.replace("MB", ""))
        elif "GB" in size:
                return int(size.replace("GB", "")) * 1024


def lookup_dataset(job, job_size):
        dataset = ""
        if job == "terasort":
                dataset = "terasort"
        elif job == "classification":
                dataset = "kmeans"
        elif job == "histogrammovies":
                dataset = "kmeans"
        elif job == "histogramratings":
                dataset = "kmeans"
        else:
                dataset = "wikipedia"
        return "/dataset/%s_%s" % (dataset, lookup_size_name(job_size))

def get_conf_dir():
    conf_dir = os.path.join(os.getcwd(), "conf")
    return conf_dir

def get_timestamp():
    now = datetime.datetime.now()
    time_string = now.strftime("%Y%m%d%H%M%S")
    return time_string

def get_output_dir(job_name, job_size, identifier=None):
    if identifier is None:
        return "/output/%s_%s" % (job_name, job_size)
    else:
        return "/output/%s_%s_%s" % (job_name, job_size, identifier)


def get_log_path(job_name, job_size, identifier=None):
    # turnaround solution
    log_dir = os.path.join(os.getcwd(), "log")
    if identifier is None:
        return os.path.join(log_dir, "%s_%s.log" % (job_name, job_size))
    else:
        return os.path.join(log_dir, "%s_%s_%s.log" % (job_name, job_size, identifier))

def get_returncode_path(log_path):
    return log_path.replace(".log", ".returncode")

def get_dataset_dir():
    return "/dataset"

def get_dataset_source_dir():
    return "/nfs_power2/dataset"

def get_dataset_name(job_name):
    dataset_name = None
    if job_name == "terasort":
        dataset_name = "terasort"
    elif job_name == "kmeans":
        dataset_name = "kmeans"
    elif job_name == "histogrammovies":
        dataset_name = "kmeans"
    elif job_name == "histogramratings":
        dataset_name = "kmeans"
    else:
        dataset_name = "wikipedia"
    return dataset_name



def get_hadoop_setting(model, num_nodes, num_storages, scheduler):
    setting_dir = os.path.join(os.getcwd(), "setting")
    cluster_config_path = os.path(setting_dir, "cluster_%s_%sc%s.py" % (model, num_nodes, num_storages))
    node_config_path = os.path(setting_dir, "node_config.py")
    hadoop_setting = base.HadoopSetting(cluster_config_path, node_config_path, scheduler=scheduler, model=model, num_nodes=num_nodes, num_storages=num_storages)
    return hadoop_setting

def get_hadoop_dir():
    return "~/hadoop"
