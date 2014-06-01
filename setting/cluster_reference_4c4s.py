user = 'chinjunh'

mapreduce = {
        'ResourceManager': 'KSWAD-scale64.ict.englab.netapp.com',
        'NodeManagers': [
                'KSWAD-scale64.ict.englab.netapp.com',
		'KSWAD-scale65.ict.englab.netapp.com',
		'KSWAD-scale66.ict.englab.netapp.com',
		'KSWAD-scale67.ict.englab.netapp.com',
        ]
}

hdfs = {
        'NameNode': 'KSWAD-scale64.ict.englab.netapp.com',
        'DataNodes': [
		'KSWAD-scale64.ict.englab.netapp.com',
                'KSWAD-scale65.ict.englab.netapp.com',
                'KSWAD-scale66.ict.englab.netapp.com',
                'KSWAD-scale67.ict.englab.netapp.com',
        ]
}

historyserver = {
        'host': 'KSWAD-scale64.ict.englab.netapp.com',
        'port': '19888',
}

ganglia = "KSWAD-scale64.ict.englab.netapp.com"
