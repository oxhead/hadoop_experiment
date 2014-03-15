mapreduce = {
	'ResourceManager': 'power6.csc.ncsu.edu',
	'NodeManagers': [
		'power2.csc.ncsu.edu',
                'power3.csc.ncsu.edu',
                'power4.csc.ncsu.edu',
		'power5.csc.ncsu.edu',
	]
}

hdfs = {
	'NameNode': 'power6.csc.ncsu.edu',
	'DataNodes': [
		'power2.csc.ncsu.edu',
                'power3.csc.ncsu.edu',
                'power4.csc.ncsu.edu',
                'power5.csc.ncsu.edu',
	]
}

historyserver = {
        'host': 'power6.csc.ncsu.edu',
        'port': '19888',
}

ganglia = "power1.csc.ncsu.edu"
