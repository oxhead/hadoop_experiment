# Silver #
A framework to run jobs and collect data in Hadoop

### Requirements ###
* Python 2.7+
* Python library - argpars, requests

### Directory Structure ###
* src - Python library
* conf - Hadoop configuration template
* setting - cluster configuration

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


