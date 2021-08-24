# Cassandra Tools
[TOC]

## CfStats Parser

Script to convert `nodetool cfstats` into a tabular Excel sheet. This improves readability and helps on the analysis.

### Requirements
Those scripts were tested on Python 3.8.
To install the required packages, just run:
```bash
$ pip install -r requirements.txt
```

### How to run?
#### CfStats parser
Run the following command, where `inputFile` is the full path for the file containing CfStats output.
```bash
cfsStatsParser.py [-h] [-d] inputFile
```

You can use the option `-h` to get help about the sintax and `-d` to enable de debug mode.

#### CQL Schema parser
Run the following command, where `inputFile` is the full path for the file containing CfStats output.
```bash
cqlSchemaParser.py [-h] [-d] inputFile
```

You can use the option `-h` to get help about the sintax and `-d` to enable de debug mode.