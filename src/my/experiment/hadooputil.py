import os
import random
import logging

from my.hadoop import service
from my.hadoop import config
from my.experiment import helper
from my.util import command

logger = logging.getLogger(__name__)


def switch_config(setting, format=False):

        cluster_config_path = setting.cluster_config_path
        node_config_path = setting.node_config_path
        scheduler = setting.scheduler

        logger.info("Switch Hadoop cofiguration")
        logger.info("Cluster: %s", cluster_config_path)
        logger.info("Node: %s", node_config_path)
        logger.info("Scheduer: %s", scheduler)

        conf_dir = helper.get_conf_dir()
        conf_generated = "myconf"

        cluster_config = config.get_cluster(cluster_config_path)
        (scheduler_class,
         scheduler_parameter) = helper.lookup_scheduler(scheduler)

        addtional_config = {
            'yarn.resourcemanager.scheduler.class': scheduler_class,
            'yarn.scheduler.flow.assignment.model': scheduler_parameter,
        }

        addtional_config = dict(setting.parameters.items() + addtional_config.items())
        logger.debug("additional paremeters: %s", addtional_config)

        logger.info("Status: stop Hadoop service")
        service.execute(cluster_config, "all", "stop")

        config.generate_config_files(
            cluster_config_path, node_config_path, conf_dir, conf_generated,
            addtional_config)

        logger.info("Status: deploy Hadoop service")
        service.deploy(cluster_config, conf_generated)

        if format:
            logger.info("Status: format HDFS storage")
            service.execute(cluster_config, "hdfs", "format", setting.node_config_path)

        logger.info("Status: start Hadoop service")
        service.execute(cluster_config, "all", "start")

        logger.info("Status: completed configuring Hadoop")


def shutdown(cluster):
    logger.info("Status: shutdown Hadoop service")
    service.execute(cluster, "all", "stop")


def prepare_data(cluster, jobs):
        logger.info("Prepaing data")
        node_list = cluster.getNodes()
        data_list = []
        for job in jobs:
                dataset_name = helper.get_dataset_name(job.name)
                data_list.append((dataset_name, job.size))

        for (dataset_name, dataset_size) in list(set(data_list)):
                upload_data(cluster, dataset_name, dataset_size)


def upload_data(cluster, dataset_name, dataset_size):
        logger.info("Prepare data for %s: %s", dataset_name, dataset_size)

        hadoop_dir = helper.get_hadoop_dir()
        dataset_dir = helper.get_dataset_dir()
        dataset_source_dir = helper.get_dataset_source_dir()
        target_dir = "%s_%s" % (dataset_name, dataset_size)
        rm_host = cluster.getMapReduceCluster().getResourceManager().host

        command.execute_remote(cluster.getUser(), rm_host, "%s/bin/hadoop dfs -mkdir -p %s" % (hadoop_dir, dataset_dir))
        if dataset_name == "terasort":
            real_size = helper.convert_unit(dataset_size)
            num_rows = real_size * 1024 * 1024 / 100
            num_files = 1 if real_size <= 1024 else real_size / 1024
            output_dir = "%s/%s" % (dataset_dir, target_dir)
            command.execute_remote(cluster.getUser(), rm_host, "%s/bin/hadoop jar %s/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar teragen -Dmapreduce.job.maps=%s %s  %s" % (hadoop_dir, hadoop_dir, num_files, num_rows, output_dir))

        else:
            command.execute_remote(cluster.getUser(), rm_host, "%s/bin/hadoop dfs -mkdir -p %s/%s" % (hadoop_dir, dataset_dir, target_dir))
            for f in os.listdir(os.path.join(dataset_source_dir, target_dir)):
                data_nodes = cluster.getHDFSCluster().getDataNodes()
                node = data_nodes[random.randint(0, len(data_nodes) - 1)]
                logger.info("upload %s/%s to node %s", target_dir, f, node.host)
                command.execute_remote(cluster.getUser(), node.host, "%s/bin/hadoop dfs -put %s/%s/%s %s/%s" % (hadoop_dir, dataset_source_dir, target_dir, f, dataset_dir, target_dir))


def generate_command(job):
        cmd = None

        if "wordcount" == job.name:
                cmd = "%s/bin/hadoop jar %s/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar wordcount -Dmapreduce.job.reduces=%s -Dmapreduce.map.memory.mb=%s %s %s > %s 2>&1 ; echo $? > %s" % (
                    job.hadoop_dir, job.hadoop_dir, job.num_reducers, job.map_size, job.input_dir, job.output_dir, job.log, job.returncode)
        elif "invertedindex" == job.name:
                cmd = "%s/bin/hadoop jar %s/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar invertedindex -Dmapreduce.job.reduces=%s -Dmapreduce.map.memory.mb=%s %s %s > %s 2>&1 ; echo $? > %s" % (
                    job.hadoop_dir, job.hadoop_dir, job.num_reducers, job.map_size, job.input_dir, job.output_dir, job.log, job.returncode)
        elif "termvector" == job.name:
                cmd = "%s/bin/hadoop jar %s/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar termvector -Dmapreduce.job.reduces=%s -Dmapreduce.map.memory.mb=%s %s %s > %s 2>&1 ; echo $? > %s" % (
                    job.hadoop_dir, job.hadoop_dir, job.num_reducers, job.map_size, job.input_dir, job.output_dir, job.log, job.returncode)
        elif "histogrammovies" == job.name:
                cmd = "%s/bin/hadoop jar %s/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar histogrammovies -Dmapreduce.job.reduces=%s -Dmapreduce.map.memory.mb=%s %s %s > %s 2>&1 ; echo $? > %s" % (
                    job.hadoop_dir, job.hadoop_dir, job.num_reducers, job.map_size, job.input_dir, job.output_dir, job.log, job.returncode)
        elif "histogramratings" == job.name:
                cmd = "%s/bin/hadoop jar %s/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar histogramratings -Dmapreduce.job.reduces=%s -Dmapreduce.map.memory.mb=%s %s %s > %s 2>&1 ; echo $? > %s" % (
                    job.hadoop_dir, job.hadoop_dir, job.num_reducers, job.map_size, job.input_dir, job.output_dir, job.log, job.returncode)
        elif "grep" == job.name:
                pattern = "hadoop.*"
                cmd = "%s/bin/hadoop jar %s/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar grep -Dmapreduce.job.reduces=%s -Dmapreduce.map.memory.mb=%s %s %s \"%s\" > %s 2>&1 ; echo $? > %s" % (
                    job.hadoop_dir, job.hadoop_dir, job.num_reducers, job.map_size, job.input_dir, job.output_dir, pattern, job.log, job.returncode)
        elif "grep2" == job.name:
                pattern = ".*hadoop.*"
                cmd = "%s/bin/hadoop jar %s/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar grep -Dmapreduce.job.reduces=%s -Dmapreduce.map.memory.mb=%s %s %s \"%s\" > %s 2>&1 ; echo $? > %s" % (
                    job.hadoop_dir, job.hadoop_dir, job.num_reducers, job.map_size, job.input_dir, job.output_dir, pattern, job.log, job.returncode)
        elif "terasort" == job.name:
                cmd = "%s/bin/hadoop jar %s/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar terasort -Dmapreduce.job.reduces=%s -Dmapreduce.map.memory.mb=%s %s %s > %s 2>&1 ; echo $? > %s" % (
                    job.hadoop_dir, job.hadoop_dir, job.num_reducers, job.map_size, job.input_dir, job.output_dir, job.log, job.returncode)
        elif "nocomputation" == job.name:
                cmd = "%s/bin/hadoop jar %s/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar nocomputation -Dmapreduce.job.reduces=%s -Dmapreduce.map.memory.mb=%s %s %s > %s 2>&1 ; echo $? > %s" % (
                    job.hadoop_dir, job.hadoop_dir, job.num_reducers, job.map_size, job.input_dir, job.output_dir, job.log, job.returncode)
        elif "custommap" in job.name:
                timeout = 1
                params = job.params
                if len(job.name) > 9:
                        timeout = int(job.name[9:])
                        if params is None:
                                params = {
                                    'timeout': timeout, 'num_cpu_workers': timeout,
                                    'num_vm_workers': timeout, 'vm_bytes': str(1024 * 1024 * timeout)}

                cmd = "%s/bin/hadoop jar %s/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar custommap -Dmapreduce.job.reduces=%s -Dmapreduce.map.memory.mb=%s %s %s %s %s %s %s > %s 2>&1 ; echo $? > %s" % (
                    job.hadoop_dir, job.hadoop_dir, job.num_reducers, job.map_size, job.input_dir, job.output_dir, params['timeout'], params['num_cpu_workers'], params['num_vm_workers'], params['vm_bytes'], job.log, job.returncode)

        elif "classification" == job.name:
                num_mapper = job.size / 64
                num_reducer = num_mapper / 8 if num_mapper / 8 > 0 else 1
                cmd = "%s/bin/hadoop jar %s/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar classification -m %s -r %s %s %s >%s 2>&1 ; echo $? > %s" % (
                    job.hadoop_dir, job.hadoop_dir, num_mapper, num_reducer, job.input_dir, job.output_dir, job.log, job.returncode)
        return cmd
