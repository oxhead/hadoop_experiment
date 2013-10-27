#!/usr/bin/python

import sys
import os
import argparse
import tempfile
import datetime

metric_list = ["cpu_report", "network_report"]

#@TODO: add time range
def collect(gangalia_server, cluster, node_list, output_directory):

	collect_time = datetime.datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
	export_dir = os.path.join(output_directory, collect_time)
	os.system("mkdir -p %s" % export_dir)
        for metric in metric_list:
                for node in node_list:
                        export_file = os.path.join(export_dir, "%s_%s.csv" % (node, metric))
                        cmd = "curl --silent 'http://%s/ganglia2/graph.php?c=%s&h=%s&n=1&v=&m=%s&csv=1' > %s" % (gangalia_server, cluster, node, metric, export_file)
                        print cmd
                        os.system(cmd)

def main(argv):

        input_dir = ''
        output_dir = ''

        parser = argparse.ArgumentParser(description='Collect data from Ganglia web 2')

        parser.add_argument("-g", '--ganglia', required=True, help="The address of the Ganglia server")
        parser.add_argument("-c", '--cluster', required=True, help="The name of the cluster")
        parser.add_argument("-n", '--node', action="append", required=True, help="The name of the host")
        parser.add_argument("-d", '--directory', required=True, help="The output directory")


        args = parser.parse_args()

        collect(args.ganglia, args.cluster, args.node, args.directory)

if __name__ == "__main__":
        main(sys.argv[1:])
