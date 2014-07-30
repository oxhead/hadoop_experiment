config = {
        'KSWAD-scale67.ict.englab.netapp.com': {
                'yarn.nodemanager.resource.memory-mb': '49500',
                'yarn.scheduler.minimum-allocation-mb': '1024',
                'JAVA_HOME': '/usr/java/jdk1.7.0_10',
                'hdfs.datanode.dir': '[SSD]/ssd/mapred/data_10/chinjunh/data, [SSD]/ssd/mapred/data_11/chinjunh/data, [SSD]/ssd/mapred/data_12/chinjunh/data, [SSD]/ssd/mapred/data_13/chinjunh/data',
                'hadoop.runtime.dir': "/ssd/mapred/data_3/chinjunh/hadoop_runtime",
                'dfs.replication': '1',
                'mapreduce.cluster.local.dir': '/ssd/mapred/data_10/chinjunh/mapreduce/local, /ssd/mapred/data_11/chinjunh/mapreduce/local, /ssd/mapred/data_12/chinjunh/data'
	},
        'KSWAD-scale(.*).ict.englab.netapp.com': {
                'yarn.nodemanager.resource.memory-mb': '49500',
                'yarn.scheduler.minimum-allocation-mb': '1024',
                'JAVA_HOME': '/usr/java/jdk1.7.0_10',
		'hdfs.datanode.dir': '[DISK]/data/d20/chinjunh/data, [DISK]/data/d21/chinjunh/data, [DISK]/data/d22/chinjunh/data, [DISK]/data/d23/chinjunh/data, [DISK]/data/d24/chinjunh/data,[DISK]/data/d25/chinjunh/data,[DISK]/data/d26/chinjunh/data,[DISK]/data/d27/chinjunh/data,[DISK]/data/d28/chinjunh/data,[DISK]/data/d29/chinjunh/data,[DISK]/data/d30/chinjunh/data,[DISK]/data/d31/chinjunh/data,[DISK]/data/d32/chinjunh/data,[DISK]/data/d33/chinjunh/data',
		'hadoop.runtime.dir': "/ssd/mapred/data_3/chinjunh/hadoop_runtime1",
                'yarn.nodemanager.local-dirs': '/ssd/mapred/data_3/chinjunh/hadoop_runtime1/yarn/nodemanager/local, /ssd/mapred/data_3/chinjunh/hadoop_runtime2/yarn/nodemanager/local, /ssd/mapred/data_4/chinjunh/hadoop_runtime/yarn/nodemanager/local',
		'dfs.replication': '1',
        },
}
