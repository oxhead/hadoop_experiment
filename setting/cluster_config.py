user = 'root'

mapreduce = {
        'ResourceManager': '10.25.13.31',
        'NodeManagers': [
                '10.25.13.35',
                '10.25.13.39',
                '10.25.13.46',
        ]
}

hdfs = {
        'NameNode': '10.25.13.31',
        'DataNodes': [
		'10.25.13.36',
                '10.25.13.47',
                '10.25.13.48',
        ]
}

historyserver = {
        'host': '10.25.13.31',
        'port': '19888',
}

ganglia = "10.25.13.31"
