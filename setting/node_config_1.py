config = {
        'KSWAD-scale(.*).ict.englab.netapp.com': {
                'yarn.nodemanager.resource.memory-mb': '49500',
                'yarn.scheduler.minimum-allocation-mb': '1024',
                'JAVA_HOME': '/usr/java/jdk1.7.0_10',
		'hdfs.datanode.dir': '[SSD]/test/ssd/data, [DISK]/test/hdd_1/data',
		'hadoop.runtime.dir': "/test/ssd/hadoop_runtime",
                'yarn.nodemanager.local-dirs': '/test/ssd/hadoop_runtime/yarn/nodemanager/local',
		'dfs.replication': '1',
        },
}
