user = 'root'

mapreduce = {
        'ResourceManager': '10.25.13.41',
        'NodeManagers': [
                '10.25.13.31',
                '10.25.13.32',
                '10.25.13.37',
                '10.25.13.38',
        ]
}

hdfs = {
        'NameNode': '10.25.13.41',
        'DataNodes': [
		'10.25.13.34',
                '10.25.13.36',
        ]
}

historyserver = {
        'host': '10.25.13.41',
        'port': '19888',
}

ganglia = "10.25.13.41"
