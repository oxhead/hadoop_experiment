import time
import datetime
import time
import logging
import copy
import os
from my.util import command
from my.experiment import hadooputil
from my.experiment import helper
from my.experiment import jobutil
from my.experiment import monitoringtool
from my.experiment import reporttool
from my.experiment import historytool
from my.datastore import db
from my.util import jsonutil

class Controller():

    def __init__(self, experiment):
        self.experiment = experiment
        self.jobs = []

    @staticmethod
    def from_snapshot(snapshot, id, output_dir):
        experiment = jsonutil.import_json(snapshot)
        experiment.id = id
        old_output_dir = experiment.output_dir
        experiment.output_dir = output_dir
        # replace output_dir with new one
        for (job_time, job_list) in experiment.job_timeline.iteritems():
            for job in job_list:
                job.log_path = job.log_path.replace(old_output_dir, output_dir)
                job.returncode_path = job.returncode_path.replace(old_output_dir, output_dir)
        return Controller(experiment)

    @property
    def logger(self):
        name = '.'.join([__name__, self.__class__.__name__])
        return logging.getLogger(name)

    def get_path(self, kind):
        if kind == 'ganglia': return "%s/ganglia" % self.experiment.output_dir
        elif kind == 'dstat': return "%s/dstat" % self.experiment.output_dir
        elif kind == 'history': return "%s/history" % self.experiment.output_dir
        elif kind == 'snapshot': return "%s/snapshot" % self.experiment.output_dir
        elif kind == 'log': return "%s/log" % self.experiment.output_dir

    def mkdirs(self, path):
        if not os.path.exists(path): os.makedirs(path)

    def init(self):
        command.execute("mkdir -p %s" % self.experiment.output_dir)

        # create unique job set
        self.jobs = []
        for (job_time, job_list) in self.experiment.job_timeline.iteritems():
            for job in job_list:
                self.jobs.append(job)

        # create required dirs
        self.mkdirs(self.get_path('log')) # actually job can have separate log dir ant it's for convinence here
        self.mkdirs(self.get_path('dstat'))
        self.mkdirs(self.get_path('history'))
        self.mkdirs(self.get_path('snapshot'))

        hadooputil.switch_config(self.experiment.hadoop_setting, self.experiment.format)
        time.sleep(60)
        if self.experiment.upload:
            hadooputil.prepare_data(self.experiment.cluster, self.jobs)

        self.monitor = monitoringtool.Monitor(self.experiment.cluster, self.get_path('dstat'))

    def run(self):
        self.init()
        self.start()
        self.stop()

    def start(self):
        self.time_start = int(time.time())
        self.monitor.start()
        self.logger.debug(self.experiment.job_timeline)
        current_time = 0
        for key in sorted(self.experiment.job_timeline.keys()):
            value = self.experiment.job_timeline[key]
            if (int(key) > current_time):
                time.sleep(int(key) - current_time)
            current_time = int(key)

            for i in range(len(value)):
                job = value[i]
                if self.experiment.sync:
                    jobutil.submit(job)
                else:
                    jobutil.submit_async(job)

        if not self.experiment.sync:
            jobutil.wait_completion(self.jobs)

        for job in self.jobs:
            jobutil.complete_job_info(job)

    def stop(self):
        self.time_end = int(time.time())

        self.logger.info("[Status] Wait for completion of HDFS service (60 sec)")

        time.sleep(60)
        self.monitor.stop()

        if self.experiment.export:
            self.__export()

        #self.__report()

        hadooputil.shutdown(self.experiment.hadoop_setting)

        self.logger.info("[Time] Start  : %s" % datetime.datetime.fromtimestamp(self.time_start).strftime('%Y-%m-%d %H:%M:%S'))
        self.logger.info("[Time] End    : %s" % datetime.datetime.fromtimestamp(self.time_end).strftime('%Y-%m-%d %H:%M:%S'))
        self.logger.info("[Time] Elapsed: %s sec" % (self.time_end - self.time_start))

    def take_snapshot(self):
        self.mkdirs(self.get_path('snapshot'))
        experiment_json = "%s/experiment.json" % self.get_path('snapshot')
        jsonutil.export_json(self.experiment, experiment_json)

    def __export(self):
        self.logger.info("[History] export history data as json")
        output_json = "%s/history.json" % self.get_path('history')
        # the time is not accurate as on history server
        description = self.experiment.description
        is_completed = True
        completion_time = self.time_end - self.time_start
        self.logger.info('Export data from history server')
        history_json = historytool.dump(self.experiment.cluster.getHistoryServer(), time_start=self.time_start, time_end=self.time_end)
        self.logger.info('Write data as a json file')
        historytool.writeJsonToFile(history_json, output_json)
        self.logger.info('Write history date to database')
        db.importJsonToDatabase(history_json, "store/history.db", self.experiment.id, description, is_completed, completion_time)

        # backup all configurations
        self.take_snapshot()

    def __report(self):
        self.logger.info("[Report] Job Analysis")

        output_file = "%s/measure_imbalance.csv" % self.experiment.output_dir
        output_waiting_time = "%s/waiting_time.csv" % self.experiment.output_dir
        output_flow_timeline = "%s/flow_timeline.csv" % self.experiment.output_dir
        output_task_timeline = "%s/task_timeline.csv" % self.experiment.output_dir
        output_job_analysis = "%s/job_analysis.csv" % self.experiment.output_dir
        output_progress_timeline = "%s/progress_timeline.csv" % experiment.self.output_dir

        try:
            reporttool.report_job_analysis(self.experiment.cluster, self.jobs, output_job_analysis)
        except Exception as e:
            logging.exception("Unable to create job report")

        self.logger.info("[Report] Waiting time")
        try:
            reporttool.report_waiting_time(self.experiment.cluster, self.jobs, output_waiting_time)
        except Exception as e:
            logging.exception("Unable to create waiting time report")

        self.logger.info("[Report] Task timeline")
        try:
            reporttool.report_task_timeline(self.experiment.cluster, self.jobs, output_task_timeline)
        except Exception as e:
            logging.exception("Unable to create task timeline report")

        self.logger.info("[Report] Flow timeline")
        try:
            reporttool.report_flow_timeline(self.experiment.cluster, self.jobs, output_flow_timeline)
        except Exception as e:
            logging.exception("Unable to create flow timeline report")

        self.logger.info("[Report] Progress Analysis")
        try:
            reporttool.report_progress_timeline(self.experiment.cluster, self.jobs, output_progress_timeline)
        except Exception, e:
            logging.exception("Unable to create progress report")

    def clean(self):
        for job in self.jobs:
            jobutil.clean_job(job)
