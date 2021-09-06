import os
import re
import sys
import requests
import logging
import argparse
import yaml
import json
from deepdiff import DeepDiff

# Apache repo brancg name list
cass_version = ['cassandra-4.0',
        'cassandra-3.11',
        'cassandra-3.0',
        'cassandra-2.2',
        'cassandra-1.0',
        'cassandra-1.1',
        'cassandra-1.2',
        'cassandra-2.0',
        'cassandra-2.1']

def main():
    # Command line arguments parser
    parser = argparse.ArgumentParser()
    parser.add_argument("inputFile", help="")
    parser = argparse.ArgumentParser()
    parser.add_argument("cassandraVersion", help="Cassandra version (1.0, 1.1, 1.2, 2.0, 2.1, 2.2, 3.0, 3.11)")
    parser.add_argument("inputFile", help="Path to cassandra.yaml")
    parser.add_argument("-o", "--offline", action="store_true", help="Disable live file download")
    parser.add_argument("-d", "--debug", action="store_true", help="Increase output verbosity")
    args = parser.parse_args()

    # Parsing args
    input_file = args.inputFile
    selected_version = args.cassandraVersion

    # Controls debug mode
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    # Getting current folder and creating configuration folder
    cwd = os.getcwd()
    conf_folder = os.path.join(cwd, 'conf')
    if not os.path.exists(conf_folder):
        os.makedirs(conf_folder)

    # Downloading all config files from Apache repo
    if not args.offline:
        logging.info('Downloading cassandra.yaml from Apache repository.')
        for branch in cass_version:
            if selected_version in branch:
                url = 'https://raw.githubusercontent.com/apache/cassandra/{branchName}/conf/cassandra.yaml'.format(branchName=branch)
                r = requests.get(url, allow_redirects=True)

                full_file_path = outputKeyspaceFile = os.path.join(conf_folder, '{branchName}.yaml'.format(branchName=branch))
                open(full_file_path, 'wb').write(r.content) 
    
    # Compare files
    logging.info('Comparing files.')
    full_file_path = outputKeyspaceFile = os.path.join(conf_folder, 'cassandra-{version}.yaml'.format(version=selected_version))

    a = yaml_as_dict(full_file_path)
    b = yaml_as_dict(input_file)
    ddiff = DeepDiff(a, b, ignore_order=True)

    # Format output
    logging.info('Formatting output.')
    jsoned = ddiff.to_json()
    pretty_json = json.dumps(json.loads(jsoned), indent=4, sort_keys=True)

    # Exporting file
    logging.info('Exporting file.')

    f = open("cassandra-yaml-diff.json", "w")
    f.write(pretty_json)
    f.close()

    #print(pretty_json)


def yaml_as_dict(my_file):
    my_dict = {}
    with open(my_file, 'r') as fp:
        docs = yaml.safe_load_all(fp)
        for doc in docs:
            for key, value in doc.items():
                my_dict[key] = value
    return my_dict


if __name__ == "__main__":
    main()

