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

Measure imbalance
* python2.7 measure_imbalance.py -d results/test1 -i 1 -n 4 -m decoupled
