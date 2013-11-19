A way to automate your experiments on Hadoop
==========

Requirements
* Python 2.7+

Supported Tasks
* python2.7 batch.py --hadoop ~/hadoop --nfs /nfs_power2 --model decoupled --model reference -d MEM_50GB_2
* python2.7 batch.py --hadoop ~/hadoop --nfs /nfs_power2 -d ~/output --model decoupled --job grep -s 50GB --slot 64 --reducer 1 --scheduler InMemory --transfer True
