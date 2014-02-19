import generate_topology
import load_data
def load():
        cluster = generate_topology.generate(load_data.getMapReduceClusterConfig(), load_data.getHDFSClusterConfig())
        mapreduce = cluster.mapreduce
        hdfs = cluster.hdfs
        return cluster
