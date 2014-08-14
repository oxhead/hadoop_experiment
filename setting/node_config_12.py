config = {
        'KSWAD-scale(.*).ict.englab.netapp.com': {
                'yarn.nodemanager.resource.memory-mb': '49500',
                'yarn.scheduler.minimum-allocation-mb': '1024',
                'JAVA_HOME': '/usr/java/jdk1.7.0_10',
		'hdfs.datanode.dir': '[DISK]/storage/data_hdd/part1/data, [DISK]/storage/data_hdd/part2/data, [DISK]/storage/data_hdd/part3/data, [DISK]/storage/data_hdd/part4/data, [DISK]/storage/data_hdd/part5/data, [SSD]/storage/data_ssd_1/data',
		'hadoop.runtime.dir': "/storage/shuffle_ssd_1/chinjunh/hadoop_runtime",
                'yarn.nodemanager.local-dirs': '/storage/shuffle_ssd_1/chinjunh/hadoop_runtime/yarn/nodemanager/local',
		'dfs.replication': '1',
        },
}
