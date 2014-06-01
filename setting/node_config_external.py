config = {
        '10.113.211.(.*)': {
                'yarn.nodemanager.resource.memory-mb': '49500',
                'yarn.scheduler.minimum-allocation-mb': '1024',
                'JAVA_HOME': '/usr/java/jdk1.7.0_10',
		'hdfs.datanode.dir': '[SSD]/ssd/mapred/data_3/chinjunh/data, [DISK]/data/d20/chinjunh/data,[DISK]/data/d21/chinjunh/data,[DISK]/data/d22/chinjunh/data,[DISK]/data/d23/chinjunh/data, [DISK]/data/d24/chinjunh/data,[DISK]/data/d25/chinjunh/data,[DISK]/data/d26/chinjunh/data,[DISK]/data/d27/chinjunh/data,[DISK]/data/d28/chinjunh/data,[DISK]/data/d29/chinjunh/data,[DISK]/data/d30/chinjunh/data,[DISK]/data/d31/chinjunh/data,[DISK]/data/d32/chinjunh/data,[DISK]/data/d33/chinjunh/data',
		'hadoop.runtime.dir': "/ssd/mapred/data_3/chinjunh/hadoop_runtime",
        },
}
