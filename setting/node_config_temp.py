config = {
        'KSWAD-scale(.*).ict.englab.netapp.com': {
                'yarn.nodemanager.resource.memory-mb': '49500',
                'yarn.scheduler.minimum-allocation-mb': '1024',
                'JAVA_HOME': '/usr/java/jdk1.7.0_10',
		'hdfs.datanode.dir': '[DISK]/home/chinjunh/hadoop_runtim/data',
		'hadoop.runtime.dir': "/home/chinjunh/hadoop_runtime",
		'dfs.replication': '1',
		'dfs.datanode.fsdataset.volume.choosing.policy': 'org.apache.hadoop.hdfs.server.datanode.fsdataset.SSDFirstVolumeChoosingPolicy',
        },
}
