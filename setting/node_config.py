config = {
        '10.25.*.*': {
                #'yarn.nodemanager.resource.memory-mb': '15000',
                'yarn.scheduler.minimum-allocation-mb': '4096',
                'JAVA_HOME': '/usr/lib/jvm/java-7-oracle',
                'hdfs.datanode.dir': '/root/hadoop_runtime/hdfs/datanode',
                'hadoop.runtime.dir': '/root/hadoop_runtime',
                'dfs.replication': '1',
                'mapreduce.cluster.local.dir': '/root/hadoop_runtime/cluster',
                'yarn.nodemanager.local-dirs': '/root/hadoop_runtime/yarn/local',
	},
}
