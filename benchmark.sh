#!/bin/bash

python script/benchmark.py --directory results/test_ssd_1replication --job terasort --size 1GB --size 2GB --size 4GB --size 8GB --size 16GB --size 32GB --size 64GB --size 128GB --node setting/node_config_ssd.py -p dfs.replication=1

python script/benchmark.py --directory results/test_ssd_2replication --job terasort --size 1GB --size 2GB --size 4GB --size 8GB --size 16GB --size 32GB --size 64GB --size 128GB --node setting/node_config_ssd.py -p dfs.replication=2

python script/benchmark.py --directory results/test_ssd_3replication --job terasort --size 1GB --size 2GB --size 4GB --size 8GB --size 16GB --size 32GB --size 64GB --size 128GB --node setting/node_config_ssd.py -p dfs.replication=3

python script/benchmark.py --directory results/test_hdd_1dir_1replication --job terasort --size 1GB --size 2GB --size 4GB --size 8GB --size 16GB --size 32GB --size 64GB --size 128GB --node setting/node_config_hdd_1dir.py -p dfs.replication=1

python script/benchmark.py --directory results/test_hdd_1dir_2replication --job terasort --size 1GB --size 2GB --size 4GB --size 8GB --size 16GB --size 32GB --size 64GB --size 128GB --node setting/node_config_hdd_1dir.py -p dfs.replication=2

python script/benchmark.py --directory results/test_hdd_1dir_3replication --job terasort --size 1GB --size 2GB --size 4GB --size 8GB --size 16GB --size 32GB --size 64GB --size 128GB --node setting/node_config_hdd_1dir.py -p dfs.replication=3

python script/benchmark.py --directory results/test_hdd_2dir_1replication --job terasort --size 1GB --size 2GB --size 4GB --size 8GB --size 16GB --size 32GB --size 64GB --size 128GB --node setting/node_config_hdd_2dir.py -p dfs.replication=1

python script/benchmark.py --directory results/test_hdd_2dir_2replication --job terasort --size 1GB --size 2GB --size 4GB --size 8GB --size 16GB --size 32GB --size 64GB --size 128GB --node setting/node_config_hdd_2dir.py -p dfs.replication=2

python script/benchmark.py --directory results/test_hdd_2dir_3replication --job terasort --size 1GB --size 2GB --size 4GB --size 8GB --size 16GB --size 32GB --size 64GB --size 128GB --node setting/node_config_hdd_2dir.py -p dfs.replication=3

python script/benchmark.py --directory results/test_hdd_3dir_1replication --job terasort --size 1GB --size 2GB --size 4GB --size 8GB --size 16GB --size 32GB --size 64GB --size 128GB --node setting/node_config_hdd_3dir.py -p dfs.replication=1

python script/benchmark.py --directory results/test_hdd_3dir_2replication --job terasort --size 1GB --size 2GB --size 4GB --size 8GB --size 16GB --size 32GB --size 64GB --size 128GB --node setting/node_config_hdd_3dir.py -p dfs.replication=2

python script/benchmark.py --directory results/test_hdd_3dir_3replication --job terasort --size 1GB --size 2GB --size 4GB --size 8GB --size 16GB --size 32GB --size 64GB --size 128GB --node setting/node_config_hdd_3dir.py -p dfs.replication=3
