import os
import re
import sys
import logging
import numpy as np
import pandas as pd 
import argparse

from datetime import datetime

# Global variables
keyspaces = []
tables_header = []
tables = []

def main():
    # Command line arguments parser
    parser = argparse.ArgumentParser()
    parser.add_argument("inputFile", help="")
    parser = argparse.ArgumentParser()
    parser.add_argument("inputFile", help="Path to a file contaning nodetool cfstats output")
    parser.add_argument("-d", "--debug", action="store_true", help="increase output verbosity")
    args = parser.parse_args()
    
    # Controls debug mode
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
    
    # Gets the file from the inputed argument
    inputFile = args.inputFile
    with open(inputFile, 'r') as fp:
        print('Processing file...') 
        content = fp.read()

    # Initialize lists
    build_lists()
    
    # Fill keyspace and table lists 
    fill_keyspaces(content)
    fill_tables(content)

    # Export CSV
    export_to_excel()

def build_lists():
    global keyspaces
    global tables
    global tables_header

    keyspaces = np.asarray(['Keyspace', 'Read Count', 'Read Latency', 'Write Count', 'Write Latency', 'Pending Flushes',  'Table Count'])
    tables_header = np.asarray(['Keyspace', 'Table', 'SSTable count', 'Space used (live)', 'Space used (total)', 'Space used by snapshots (total)'
    , 'Off heap memory used (total)', 'SSTable Compression Ratio', 'Number of partitions (estimate)', 'Memtable cell count'
    , 'Memtable data size', 'Memtable off heap memory used', 'Memtable switch count', 'Local read count'
    , 'Local read latency', 'Local write count', 'Local write latency', 'Pending flushes'
    , 'Percent repaired', 'Bloom filter false positives', 'Bloom filter false ratio', 'Bloom filter space used'
    , 'Bloom filter off heap memory used', 'Index summary off heap memory used', 'Compression metadata off heap memory used'
    , 'Compacted partition minimum bytes', 'Compacted partition maximum bytes', 'Compacted partition mean bytes'
    , 'Average live cells per slice (last five minutes)', 'Maximum live cells per slice (last five minutes)'
    , 'Average tombstones per slice (last five minutes)', 'Maximum tombstones per slice (last five minutes)'
    , 'Dropped Mutations', 'SSTables in each level', 'Table (index)'])
    tables = np.copy(tables_header)

def fill_keyspaces(content):
    logging.debug('FILL KEYSPACES: Starting the function.')
    logging.debug('FILL KEYSPACES: Content:\n'+content)
    global keyspaces

    stack = []
    newRow = []

    lines = content.split('\n')
    lines = iter(lines)

    for line in lines: 
        body = line.lstrip('\t'); 
        level = len(line) - len(body); 
        stack[level:] = (body,)

        logging.debug('FILL KEYSPACES: Processing line....\n'+str(stack))

        # Looking for keyspaces
        if len(stack) == 1 and 'Keyspace : ' in stack[0]:
            keyspaceName = body.replace('Keyspace : ', '')
            # Initializes a new row
            newRow = ['', '','', '', '', '', '']
            newRow[0] = keyspaceName

        # Looking for read count
        key = 'Read Count: '
        if len(stack) == 2 and keyspaceName in stack[0] and key in stack[1]:
            readCount = body.replace(key, '')
            newRow[1] = readCount
        
        # Looking for read latency
        key = 'Read Latency: '
        if len(stack) == 2 and keyspaceName in stack[0] and key in stack[1]:
            readLatency = body.replace(key, '')
            newRow[2] = readLatency

        # Looking for write count
        key = 'Write Count: '
        if len(stack) == 2 and keyspaceName in stack[0] and key in stack[1]:
            writeCount = body.replace(key, '')
            newRow[3] = writeCount

        # Looking for write latency
        key = 'Write Latency: '
        if len(stack) == 2 and keyspaceName in stack[0] and key in stack[1]:
            writeLatency = body.replace(key, '')
            newRow[4] = writeLatency

        # Looking for Pending Flushes (last keyspace property)
        key = 'Pending Flushes: '
        if len(stack) == 2 and keyspaceName in stack[0] and key in stack[1]:
            pendingFlushes = body.replace(key, '')
            newRow[5] = pendingFlushes
            keyspaces = np.vstack([keyspaces, newRow])

    logging.debug('FILL KEYSPACES: Finalizing the function.')

def fill_tables(content):
    logging.debug('TABLE INFO: Starting the function.')
    global tables
    global tables_header

    lines_split = content.split('\n')
    lines = iter(lines_split)
    logging.debug('TABLE INFO: lines with '+str(len(lines_split)))

    keyspaceName = ''
    numOfTables = 0
    stack = []
    newRow = []
    for line in lines: 
        body = line.lstrip('\t'); 
        level = len(line) - len(body); 
        stack[level:] = (body,)


        # Looking for tables
        if (len(stack) == 2 and ('Table:' in stack[1] or 'Table (index)' in stack[1])) or (len(stack) == 3 and ('Table:' in stack[2] or 'Table (index)' in stack[2])):
            # If newRow is not empty, append to the main list
            if newRow:
                logging.debug('TABLE INFO: Appending row')
                tables = np.vstack([tables, newRow])
                update_table_count(newRow[0], numOfTables)

            # Create a new row to store new table info
            logging.debug('TABLE INFO: Creating a new row')
            newRow = ['', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '']
            
            if 'Keyspace : ' in stack[0]:
                keyspaceName = stack[0].replace('Keyspace : ', '')
                logging.info('TABLE INFO: Getting information from keyspace "'+keyspaceName+'"')
                logging.info('TABLE INFO: =========================================================')

            # Calculating the table name position
            tableNamePos = len(stack)-1
            tableName = stack[tableNamePos].replace('Table: ', '')
            logging.info('TABLE INFO: + Table "'+tableName+'"')

            # Initializes a new row
            newRow[0] = keyspaceName
            newRow[1] = tableName
            numOfTables = numOfTables + 1
        
        # Looking for SSTable count
        if len(stack) == 3 and (newRow[1] in stack[1] or 'Pending Flushes: ' in stack[1]):
            propertyValue = stack[2].split(": ",1)[1]
            propertyName = stack[2].split(": ",1)[0]
            colIndex = np.where(tables_header == propertyName)
            colIndex = colIndex[0][0]

            logging.debug('TABLE INFO: Returned key index: '+str(colIndex))
            logging.debug('TABLE INFO: Adding '+propertyName+'='+propertyValue+' on '+str(tables_header[colIndex])+' - Position '+str(colIndex))
            
            # Verifies if the string is a volume unit and converts to MiB
            units = ['bytes', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB']
            res = [ele for ele in units if(ele in propertyValue)]
            if bool(res):
                newRow[colIndex] = str(convert_to_mib(propertyValue))+' MiB'
            else:
                newRow[colIndex] = propertyValue

    logging.debug('TABLE INFO: Finalizing the function.')
            
def update_table_count(keyspaceName, TableCount):
    # Updates table count per keyspace
    keyspaceIndex = np.where(keyspaces == keyspaceName)
    rowIndex = keyspaceIndex[0][0]
    keyspaces[rowIndex][6] = str(TableCount)

def build_tree(content):
    # This function converts a tab hierarchical file into a list 
    lines = content.split('\n')
    lines = iter(lines)
    stack = []
    for line in lines: 
        body = line.lstrip('\t'); 
        level = len(line) - len(body); 
        stack[level:] = (body,)
        print(stack)
        
def export_to_excel():
    global keyspaces
    global tables

    # Export CSV
    cwd = os.getcwd()
    date = datetime. now(). strftime("%Y%m%d%I%M%S%p")
    
    # Exporting keyspace matrix
    filename = "cfstats_keyspace_matrix_"+date+".xlsx"
    outputKeyspaceFile = os.path.join(cwd, filename)
    pd.DataFrame(keyspaces).to_excel(outputKeyspaceFile, header=False, index=False)
        
    # Exporting table matrix
    filename = "cfstats_table_matrix_"+date+".xlsx"
    outputTablesFile = os.path.join(cwd, filename)
    pd.DataFrame(tables).to_excel(outputTablesFile, header=False, index=False)   

def convert_to_mib(valueToConvert):
    finalValue = 0

    if 'bytes' in valueToConvert:
        valueToConvert = valueToConvert.replace(' bytes', '')
        valueToConvert = valueToConvert.replace(' ', '')
        valueToConvert = float(valueToConvert)
        finalValue = valueToConvert * 0.00000095367431640625
    elif 'KiB' in valueToConvert:
        valueToConvert = valueToConvert.replace(' KiB', '')
        valueToConvert = valueToConvert.replace(' ', '')
        valueToConvert = float(valueToConvert)
        finalValue = valueToConvert/1024
    elif 'MiB' in valueToConvert:
        valueToConvert = valueToConvert.replace(' MiB', '')
        valueToConvert = valueToConvert.replace(' ', '')
        valueToConvert = float(valueToConvert)
        finalValue = valueToConvert * 0.00000095367431640625
    elif 'GiB' in valueToConvert:
        valueToConvert = valueToConvert.replace(' GiB', '')
        valueToConvert = valueToConvert.replace(' ', '')
        valueToConvert = float(valueToConvert)
        finalValue = valueToConvert * 1024
    elif 'TiB' in valueToConvert:
        valueToConvert = valueToConvert.replace(' TiB', '')
        valueToConvert = valueToConvert.replace(' ', '')
        valueToConvert = float(valueToConvert)
        finalValue = valueToConvert * 1048576
    elif 'PiB' in valueToConvert:
        valueToConvert = valueToConvert.replace(' PiB', '')
        valueToConvert = valueToConvert.replace(' ', '')
        valueToConvert = float(valueToConvert)
        finalValue = valueToConvert * 1073741824

    return finalValue

if __name__ == "__main__":
    main()

