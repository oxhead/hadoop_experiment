#!/usr/bin/python

import sys
import argparse
import xml.etree.ElementTree as ET


def convert_file(intput, output):
	with open(output, 'w') as output_file:
		tree = ET.parse(intput)
		root = tree.getroot()
		
		element_metrics = root.find('meta').find('legend')
		output_file.write("timestamp")
		output_file.write(",")
		output_file.write(",".join([metric.text for metric in list(element_metrics)]))
		output_file.write("\n")

		element_data = root.find('data')

		for row in element_data.findall('row'):
			collect_time = row.find('t').text
			output_file.write(collect_time)
			output_file.write(",")
			output_file.write(",".join([v.text for v in row.findall('v')]))
			output_file.write("\n")

def main(argv):

        input_dir = ''
        output_dir = ''

        parser = argparse.ArgumentParser(description='Convert RRDTool output to csv')

        parser.add_argument("-i", '--input', required=True, help="The intput xml file")
	parser.add_argument("-o", "--output", required=True, help="The output csv file")

        args = parser.parse_args()

        convert_file(args.input, args.output)

if __name__ == "__main__":
        main(sys.argv[1:])
