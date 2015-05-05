import json

from my.experiment.base import *
from my.hadoop.base import *

def export_json(obj, file_path):
    with open(file_path, 'w') as f:
        json.dump(obj, f, default=my_encoder, indent=4)

def import_json(file_path):
    with open(file_path, 'r') as f:
        return json.load(f, object_hook=my_decoder, parse_int=int)

# http://www.diveintopython3.net/serializing.html
def my_encoder(obj):
    if isinstance(obj, Job):
        return {
            '__class__': 'Job',
            '__value__': {
                'name': obj.name,
                'size': obj.size,
                'input_dir': obj.input_dir,
                'output_dir': obj.output_dir,
                'log_path': obj.log_path,
                'returncode_path': obj.returncode_path,
                'params': obj.params,
                'map_size': obj.map_size,
                'reduce_size': obj.reduce_size,
                'num_reducers': obj.num_reducers,
                'id': obj.id,
                'status': obj.status
            }
        }
    elif isinstance(obj, ExperimentRun):
        return {
            '__class__': 'ExperimentRun',
            '__value__': {
                'job_timeline': obj.job_timeline,
                'output_dir': obj.output_dir,
                'hadoop_setting': my_encoder(obj.hadoop_setting),
                'upload': obj.upload,
                'format': obj.format,
                'sync': obj.sync,
                'id': obj.id,
                'description': obj.description,
                'export': obj.export,
            }
        }
    elif isinstance(obj, HadoopSetting):
        return {
            '__class__': 'HadoopSetting',
            '__value__': {
                'cluster': my_encoder(obj.cluster),
                'config': my_encoder(obj.config),
                'parameters': obj.parameters,
                'conf_dir': obj.conf_dir,
            }
        }
    elif isinstance(obj, Node):
        return {
            '__class__': 'Node',
            '__value__': obj.host,
        }
    elif isinstance(obj, Cluster):
        return {
            '__class__': 'Cluster',
            '__value__': {
                'user': obj.user,
                'mapreduce': my_encoder(obj.getMapReduceCluster()),
                'hdfs': my_encoder(obj.getHDFSCluster()),
                'historyserver': my_encoder(obj.getHistoryServer()),
            }
        }
    elif isinstance(obj, MapReduceCluster):
        return {
            '__class__': 'MapReduceCluster',
            '__value__': {
                'resourcemanager': my_encoder(obj.getResourceManager()),
                'nodemanagers': obj.getNodeManagers(),
            }
        }
    elif isinstance(obj, HDFSCluster):
        return {
            '__class__': 'HDFSCluster',
            '__value__': {
                'namenode': my_encoder(obj.getNameNode()),
                'datanodes': obj.getDataNodes(),
            }
        }
    elif isinstance(obj, HistoryServer):
        return {
            '__class__': 'HistoryServer',
            '__value__': {
                'host': obj.host,
                'port': obj.port,
            }
        }
    elif isinstance(obj, NodeConfig):
        return {
            '__class__': 'NodeConfig',
            '__value__': obj.config,
        }
    raise TypeError(repr(obj) + ' is not JSON serializable')

def to_bool(s):
    return s == "True" or s == "true"

def my_decoder(obj):
    if '__class__' not in obj:
        #raise TypeError(repr(obj) + ' is not JSON deserializable')
        return obj
    if obj['__class__'] == 'Job':
        job = obj['__value__']
        job_obj = Job(job['name'], job['size'], job['input_dir'], job['output_dir'], job['log_path'], job['returncode_path'], params=job['params'], map_size=int(job['map_size']), reduce_size=int(job['reduce_size']), num_reducers=int(job['num_reducers']))
        job_obj.id = job['id']
        job_obj.status = to_bool(job['status'])
        return job_obj
    elif obj['__class__'] == 'ExperimentRun':
        experiment = obj['__value__']
        experiment_obj = ExperimentRun(
            experiment['hadoop_setting'],
            experiment['job_timeline'],
            experiment['output_dir'],
            upload=to_bool(experiment['upload']),
            format=to_bool(experiment['format']),
            sync=to_bool(experiment['sync']),
            id=experiment['id'],
            description=experiment['description'],
            export=experiment['export']
        )
        return experiment_obj
    elif obj['__class__'] == 'HadoopSetting':
        setting = obj['__value__']
        setting_obj = HadoopSetting(setting['cluster'], setting['config'], setting['parameters'], setting['conf_dir'])
        return setting_obj
    elif obj['__class__'] == 'Node':
        return Node(obj['__value__'])
    elif obj['__class__'] == 'Cluster':
        cluster = obj['__value__']
        cluster_obj = Cluster(cluster['user'], cluster['mapreduce'], cluster['hdfs'], cluster['historyserver'])
        return cluster_obj
    elif obj['__class__'] == 'MapReduceCluster':
        mapreducecluster = obj['__value__']
        mapreducecluster_obj = MapReduceCluster()
        mapreducecluster_obj.setResourceManager(mapreducecluster['resourcemanager'])
        for nm in mapreducecluster['nodemanagers']:
            mapreducecluster_obj.addNodeManager(nm)
        return mapreducecluster_obj
    elif obj['__class__'] == 'HDFSCluster':
        hdfscluster = obj['__value__']
        hdfscluster_obj = HDFSCluster()
        hdfscluster_obj.setNameNode(hdfscluster['namenode'])
        for dn in hdfscluster['datanodes']:
            hdfscluster_obj.addDataNode(dn)
        return hdfscluster_obj
    elif obj['__class__'] == 'HistoryServer':
        historyserver = HistoryServer(obj['__value__']['host'], obj['__value__']['port'])
        return historyserver
    elif obj['__class__'] == 'NodeConfig': 
        return NodeConfig(obj['__value__'])
    return obj
