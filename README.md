# Cassandra Tools
This repository contains scripts to help Cassandra administrators and developers on some tasks. 

## Requirements
Those scripts were tested on Python 3.8.
To install the required packages, just run:
```bash
$ pip install -r requirements.txt
```

## How to run?
### CfStats parser
Script to convert `nodetool cfstats` into a tabular Excel sheet. This improves readability and helps on the analysis.

Run the following command, where `inputFile` is the full path for the file containing CfStats output.
```bash
cfsStatsParser.py [-h] [-d] inputFile
```

You can use the option `-h` to get help about the sintax and `-d` to enable de debug mode.

### CQL Schema parser
This script convests a CQL Schema into a tabular mode sheet in Excel.

Run the following command, where `inputFile` is the full path for the file containing CfStats output.
```bash
cqlSchemaParser.py [-h] [-d] inputFile
```

You can use the option `-h` to get help about the sintax and `-d` to enable de debug mode.