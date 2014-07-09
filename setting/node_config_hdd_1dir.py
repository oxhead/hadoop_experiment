config = {
	'KSWAD-scale67.ict.englab.netapp.com': {
                'yarn.nodemanager.resource.memory-mb': '49500',
                'yarn.scheduler.minimum-allocation-mb': '1024',
                'JAVA_HOME': '/usr/java/jdk1.7.0_10',
                'hdfs.datanode.dir': '[DISK]/data/d20/chinjunh/data',
                'hadoop.runtime.dir': "/ssd/mapred/data_10/chinjunh/hadoop_runtime",
                'dfs.datanode.fsdataset.volume.choosing.policy': 'org.apache.hadoop.hdfs.server.datanode.fsdataset.SSDFirstVolumeChoosingPolicy',
        },
        'KSWAD-scale6(.*).ict.englab.netapp.com': {
                'yarn.nodemanager.resource.memory-mb': '49500',
                'yarn.scheduler.minimum-allocation-mb': '1024',
                'JAVA_HOME': '/usr/java/jdk1.7.0_10',
		'hdfs.datanode.dir': '[DISK]/data/d20/chinjunh/data',
                'hadoop.runtime.dir': "/data/d20/chinjunh/hadoop_runtime",
		'dfs.datanode.fsdataset.volume.choosing.policy': 'org.apache.hadoop.hdfs.server.datanode.fsdataset.SSDFirstVolumeChoosingPolicy',
        },
}
