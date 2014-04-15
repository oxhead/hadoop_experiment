import os
import sys
import logging


def init(format=None):
    sys.path.append(get_lib_path())

    FORMAT = '%(filename)s:%(lineno)s:%(funcName)s() - %(message)s' if format is None else format
    logging.getLogger("requests").setLevel(logging.CRITICAL)
    logging.basicConfig(level=logging.DEBUG, format=FORMAT)

def get_project_path():
    return os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

def get_lib_path():
    return os.path.join(get_project_path(), "src")

def get_cluter_config_path(model, num_nodes, num_storages):
    cluster_config_path = os.path.join(
        get_project_path(), "setting", "cluster_%s_%sc%ss.py" % (model, num_nodes, num_storages))
    return cluster_config_path

def get_node_config_path():
    node_config_path = os.path.join(
        get_project_path(), "setting", "node_config.py")
    return node_config_path
