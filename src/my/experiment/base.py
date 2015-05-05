import time
import datetime
import time
import logging
import copy
import os

'''
Test different schedulers performance
'''
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
        #hadoop_setting = HadoopSetting()
        #myhadoop.switch_configuration(configuration, scheduler)
        time.sleep(60)
        #job_size_list = self.jobSizeList()
        #        myhadoop.prepare_data(job_size_list)
'''

class ExperimentRun():

    def __init__(self, hadoop_setting, job_timeline, output_dir, upload=False, format=False, sync=False, id=None, description='N/A', export=True):
        self.hadoop_setting = hadoop_setting
        self.job_timeline = job_timeline
        self.output_dir = output_dir
        self.cluster = hadoop_setting.cluster
        self.upload = upload
        self.format = format
        self.sync = sync
        self.export = export
        self.id = id if id is not None else datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        self.description = description
        self.time_start = None
        self.time_end = None

    @property
    def logger(self):
        name = '.'.join([__name__, self.__class__.__name__])
        return logging.getLogger(name)

    @property
    def cluster(self):
        return self.cluster

    def setSetting(self, hadoop_setting):
        self.hadoop_setting = hadoop_setting

    def getJobTimeline(self):
        return self.job_timeline

class Job(object):

    def __init__(self, name, size, input_dir, output_dir, log_path, returncode_path, params=None, map_size=1024, reduce_size=2048, num_reducers=1):
        self.name = name
        self.size = size
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.log_path = log_path
        self.returncode_path = returncode_path
        self.params = params
        self.map_size = map_size
        self.reduce_size = reduce_size
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
