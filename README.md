# Silver #
A framework to run jobs and collect data in Hadoop

### Requirements ###
* Python 2.7+
* Python library - argpars, requests

### Directory Structure ###
* src - Python library
* conf - Hadoop configuration template
* setting - cluster configuration

### Basic Scripts ###
* Generate configurations
```
python bin/config.py -n setting/node_config.py -p dfs.tier.enabled=true -p yarn.resourcemanager.scheduler.class=org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler
```
* Deploy configurations
```
python bin/config.py -n setting/node_config.py
```

* Format the cluster
```
python python bin/service.py -s all -a format
```

### Supported Scripts ###
* Measure flow demand of jobs
```
python2.7 script/measure_job.py -d results/measure_job_1
```

* Measure flow capability of computing nodes
```
python2.7 script/measure_computing.py -d results/measure_computing_1c4s --num_storages 4

```

* Measure flow capability of storage nodes
```
python2.7 script/measure_storage.py -d results/measure_storage_4c1s -s Fifo --num_nodes 4 --num_storages 1
```

* Measure weak scaling
```
python2.7 script/measure_weak_scaling.py -d results/measure_weak_scaling_1c4s -s Fifo --num_nodes 1 --num_storages 4
```

* Compare performance of schedulers
```
python2.7 script/compare_scheduler.py -d results/compare_scheduler_4c1s_n20_p15 -m decoupled -s Fifo -s ColorStorage --num_nodes 4 --num_storages 1 --num_jobs 20 --period 15
```

### Elastic ###

* Dynamically change YARN configuration
python bin/capacity.py -p yarn.scheduler.capacity.root.queues=oxhead,chsu6,default -p yarn.scheduler.capacity.root.oxhead.capacity=60 -p yarn.scheduler.capacity.root.chsu6.capacity=20 -p yarn.scheduler.capacity.root.default.capacity=20

* Simulate add/remove nodes
python bin/node.py -n 10.25.13.31 -r namenode -a [start | stop | kill | reboot | panic]

### History ###

* Console Query
python bin/history.py

  * Job.select().where(Job.name == "")
