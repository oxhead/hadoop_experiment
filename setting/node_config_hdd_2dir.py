config = {
        'KSWAD-scale6(.*).ict.englab.netapp.com': {
                'yarn.nodemanager.resource.memory-mb': '49500',
                'yarn.scheduler.minimum-allocation-mb': '1024',
                'JAVA_HOME': '/usr/java/jdk1.7.0_10',
		'hdfs.datanode.dir': '[DISK]/data/d20/chinjunh/data,[DISK]/data/d26/chinjunh/data',
                'hadoop.runtime.dir': "/data/d20/chinjunh/hadoop_runtime",
        },
}