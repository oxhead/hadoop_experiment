A way to automate your experiments on Hadoop
==========

Requirements
* Python 2.7+

Supported Tasks
* python2.7 batch.py --hadoop ~/hadoop --nfs /nfs_power2 --model decoupled --model reference -d MEM_50GB_2
* python2.7 batch.py --hadoop ~/hadoop --nfs /nfs_power2 -d ~/output --model decoupled --job grep -s 50GB --slot 64 --reducer 1 --scheduler InMemory --transfer True

Generate configurations
* python2.7 generate_configuration.py --conf conf --directory myconf

Deploy configurations
* python2.7 deploy.py --user user

Service management
* start: python2.7 service.py --user user start mapreduce
* stop: python2.7 service.py --user user start hdfs
* format: python2.7 service.py --user user format hdfs

Cluster configuration
* node_list.py: the nodes for MapReduce and HDFS
* node_configuration.py: the setting for each node above

Measurement
* Imbalance: python2.7 measure_imbalance.py -d results/imbalance_reference_4c1s_2 -i 1 -n 4 -m reference
* Weak scaling: python2.7 measure_weak_scaling.py -d results/weak_scaling -i 4 -n 4 -m decoupled
* Resource contention: python2.7 measure_contention.py -d results/contention_4c -m decoupled

Measure flow
* Flow demand: python2.7 measure_flow_demand.py -d results/flowdemand_decoupled_1c1s -m decoupled
* Flow capability (computing): python2.7 measure_computing_flow_capability.py -d results/computing-flow-capability -m decoupled
* Flow capability (storage): python2.7 measure_storage_flow_capability.py -d results/storage-flow-coapability -m decoupled

Creatr report
* python2.7 myreport.py --host power6.csc.ncsu.edu --port 19888 -t task_timeline -f output.csv
* python2.7 myreport.py --host power6.csc.ncsu.edu --port 19888 -t flow_timeline -f output.csv
* python2.7 myreport.py --host power6.csc.ncsu.edu --port 19888 -t waiting_time -f output.csv
