import time
import datetime
import time
import logging
import copy
import os
from my.util import command
from my.experiment import hadooputil
from my.experiment import jobutil
from my.experiment import monitoringtool
from my.experiment import reporttool
from my.experiment import historytool
from my.experiment.decorator import *
from my.datastore import db
from my.hadoop import config

'''
Test different schedulers performance
'''
class SchedulerExperiment():

    def __init__(self, setting, schedulers, job_timeline, output_dir):
        self.setting = setting
        self.schedulers = schedulers
        self.job_timeline = job_timeline
        self.output_dir = output_dir
        self.runs = []

    def init(self):
        for scheduler in self.schedulers:
            scheduler_setting = copy.deepcopy(self.setting)
            scheduler_setting.scheduler = scheduler
            scheduelr_output_dir = os.path.join(self.output_dir, scheduler)
            experiment = ExperimentRun(self.job_timeline, scheduelr_output_dir, setting=scheduler_setting, upload=False, format=False)
            self.runs.append(experiment)

        initial_run = self.runs[0]
        initial_setting = initial_run.setting
        hadooputil.switch_config(self.setting, True)
        time.sleep(60)
        hadooputil.prepare_data(initial_run.cluster, initial_run.jobs)

    @property
    def logger(self):
        name = '.'.join([__name__, self.__class__.__name__])
        return logging.getLogger(name)

    def run(self):
        for experiment_run in self.runs:
            try:
                experiment_run.init()
                experiment_run.run()
            except:
                self.logger.exception("Unable to compelte experiment for ")

    def prepareData(self):
        hadoop_setting = HadoopSetting()
        #myhadoop.switch_configuration(configuration, scheduler)
        time.sleep(60)
        #job_size_list = self.jobSizeList()
        #        myhadoop.prepare_data(job_size_list)


class ExperimentRun():

    def __init__(self, job_timeline, output_dir, setting=None, upload=False, format=False, sync=False):
        self.output_dir = output_dir
        self.job_timeline = job_timeline
        self.setting = setting
        self.upload = upload
        self.format = format
        self.sync = sync
        self.time_start = None
        self.time_end = None
        self.jobs = []

        self.output_ganglia = "%s/ganglia" % self.output_dir
        self.output_dstat = "%s/dstat" % self.output_dir

        self.output_file = "%s/measure_imbalance.csv" % self.output_dir
        self.output_waiting_time = "%s/waiting_time.csv" % self.output_dir
        self.output_flow_timeline = "%s/flow_timeline.csv" % self.output_dir
        self.output_task_timeline = "%s/task_timeline.csv" % self.output_dir
        self.output_job_analysis = "%s/job_analysis.csv" % self.output_dir
        self.output_progress_timeline = "%s/progress_timeline.csv" % self.output_dir

        for (job_time, job_list) in self.job_timeline.iteritems():
            for job in job_list:
                self.jobs.append(job)

        self.cluster = config.get_cluster(self.setting.cluster_config_path)

    @property
    def logger(self):
        name = '.'.join([__name__, self.__class__.__name__])
        return logging.getLogger(name)

    def setSetting(self, setting):
        self.setting = setting

    def getJobTimeline(self):
        return self.job_timeline

    def init(self):
        command.execute("mkdir -p %s" % self.output_dir)

        hadooputil.switch_config(self.setting, self.format)
        time.sleep(60)
        if self.upload:
            hadooputil.prepare_data(self.cluster, self.jobs)

        self.monitor = monitoringtool.Monitor(self.cluster, self.output_dstat)

    def run(self):
        self.start()
        self.stop()

    def start(self):
        self.time_start = int(time.time())
        self.monitor.start()
        self.logger.debug(self.job_timeline)
        current_time = 0
        for key in sorted(self.job_timeline.keys()):
            value = self.job_timeline[key]
            if (key > current_time):
                time.sleep(key - current_time)
            current_time = key

            for i in range(len(value)):
                job = value[i]
                if self.sync:
                    jobutil.submit(job)
                else:
                    jobutil.submit_async(job)

        if not self.sync:
            jobutil.wait_completion(self.jobs)

        for job in self.jobs:
            jobutil.complete_job_info(job)

    def stop(self):
        self.time_end = int(time.time())

        self.logger.info(
            "[Status] Wait for completion of HDFS service (60 sec)")

        time.sleep(60)
        self.monitor.stop()

        self.__export()

        self.__report()

        hadooputil.shutdown(self.cluster)

        self.logger.info("[Time] Start  : %s" % datetime.datetime.fromtimestamp(self.time_start).strftime('%Y-%m-%d %H:%M:%S'))
        self.logger.info("[Time] End    : %s" % datetime.datetime.fromtimestamp(self.time_end).strftime('%Y-%m-%d %H:%M:%S'))
        self.logger.info("[Time] Elapsed: %s sec" % (self.time_end - self.time_start))

    def __export(self):
        self.logger.info("[History] export history data as json")
        # the time is not accurate as on history server
        completed_date = datetime.datetime.fromtimestamp(self.time_end).strftime('%Y%m%d%H%M%S')
        experiment_id = "run_%s" % completed_date
        description = "test"
        is_completed = True
        completion_time = self.time_end - self.time_start
        history_json = historytool.dump(self.cluster.getHistoryServer(), time_start=self.time_start, time_end=self.time_end)
        historytool.writeJsonToFile(history_json, "store/history_%s.json" % experiment_id)
        db.importJsonToDatabase(history_json, "store/history.db", experiment_id, description, is_completed, completion_time)

    def __report(self):
        self.logger.info("[Report] Job Analysis")
        try:
            reporttool.report_job_analysis(
                self.cluster, self.jobs, self.output_job_analysis)
        except Exception as e:
            logging.exception("Unable to create job report")

        self.logger.info("[Report] Waiting time")
        try:
            reporttool.report_waiting_time(
                self.cluster, self.jobs, self.output_waiting_time)
        except Exception as e:
            logging.exception("Unable to create waiting time report")

        self.logger.info("[Report] Task timeline")
        try:
            reporttool.report_task_timeline(
                self.cluster, self.jobs, self.output_task_timeline)
        except Exception as e:
            logging.exception("Unable to create task timeline report")

        self.logger.info("[Report] Flow timeline")
        try:
            reporttool.report_flow_timeline(
                self.cluster, self.jobs, self.output_flow_timeline)
        except Exception as e:
            logging.exception("Unable to create flow timeline report")

        self.logger.info("[Report] Progress Analysis")
        try:
            reporttool.report_progress_timeline(
                self.cluster, self.jobs, self.output_progress_timeline)
        except Exception, e:
            logging.exception("Unable to create progress report")

    def clean(self):
        for job in self.jobs:
            jobutil.clean_job(job)


'''
Define a Hadoop cluster
'''
class HadoopSetting():

    def __init__(self, cluster_config_path, node_config_path, conf_dir="conf", scheduler="Fifo", model="decoupled", num_nodes=1, num_storages=1, parameters={}):
        self.scheduler = scheduler
        self.model = model
        self.num_nodes = num_nodes
        self.num_storages = num_storages
        self.cluster_config_path = cluster_config_path
        self.node_config_path = node_config_path
        self.conf_dir = conf_dir
        self.parameters = parameters


class Job(object):

    def __init__(self, name, size, input_dir, output_dir, log_path, returncode_path, params=None, map_size=1024, num_reducers=1):
        self.name = name
        self.size = size
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.log_path = log_path
        self.returncode_path = returncode_path
        self.params = params
        self.map_size = map_size
        self.num_reducers = num_reducers
        self.id = None
        self.status = False

    def __repr__(self):
        output = "%s_%s" % (self.name, self.size)
        return output

    def isSubmitted(self):
        return self.id is not None

    def setStatus(self, success):
        self.status = success

    def setId(self, id):
        self.id = id
