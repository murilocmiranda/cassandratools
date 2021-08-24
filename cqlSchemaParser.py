import os
import sys
import re
import sqlparse
import pandas as pd 
import numpy as np
import logging
import argparse

from datetime import datetime

# Global variables
table_info = np.asarray(['table_name' ,'table_columns','table_order','caching','comment','compaction','compression','crc_check_chance','dclocal_read_repair_chance','default_time_to_live','gc_grace_seconds','max_index_interval','memtable_flush_period_in_ms','min_index_interval','read_repair_chance','speculative_retry'])

def get_table_properties(sql):
    logging.debug('GET TABLE PROPERTIES: Starting the function.')
    global table_info

    ddls = sqlparse.split(sql)

    tmp_tables_regex = re.compile("^CREATE TABLE.*\(")
    for ddl in ddls:
        tmp_table_match = tmp_tables_regex.match(ddl)
        if tmp_table_match:
            parsed = sqlparse.parse(ddl)
            stmt = parsed[0] 

            table_name = str(stmt.tokens[4])
            print("Processing table: "+table_name)
            table_columns = str(stmt.tokens[6])
            table_order = str(stmt.tokens[14])

            caching = ''
            comment = ''              
            compaction = ''
            compression = ''
            crc_check_chance = ''
            dclocal_read_repair_chance = ''
            default_time_to_live = ''
            gc_grace_seconds = ''
            max_index_interval = ''
            memtable_flush_period_in_ms = ''
            min_index_interval = ''
            read_repair_chance = ''
            speculative_retry = ''


            # Getting table properties
            flattened = list(stmt.flatten())
            for t in reversed(flattened):
                    if t.value == "WITH":
                        # Find the location of token t in the original parsed statement
                        idx = flattened.index(t)

                        # Combine the string values of all tokens in the original list
                        text = "".join(tok.value for tok in flattened[idx + 1:-1])
                        options_splitted = text.split("AND")

                        # Parsing specific values
                        for option in options_splitted:
                            
                            option = option.strip()

                            if 'caching' in option:
                                caching = option.split(" = ")[1]
                            elif 'comment' in option:
                                comment = option.split(" = ")[1]
                            elif 'compaction' in option:
                                compaction = option.split(" = ")[1]
                            elif 'compression' in option:
                                compression = option.split(" = ")[1]
                            elif 'crc_check_chance' in option:
                                crc_check_chance = option.split(" = ")[1]
                            elif 'dclocal_read_repair_chance' in option:
                                dclocal_read_repair_chance = option.split(" = ")[1]
                            elif 'default_time_to_live' in option:
                                default_time_to_live = option.split(" = ")[1]
                            elif 'gc_grace_seconds' in option:
                                gc_grace_seconds = option.split(" = ")[1]
                            elif 'max_index_interval' in option:
                                max_index_interval = option.split(" = ")[1]
                            elif 'memtable_flush_period_in_ms' in option:
                                memtable_flush_period_in_ms = option.split(" = ")[1]
                            elif 'min_index_interval' in option:
                                min_index_interval = option.split(" = ")[1]
                            elif 'read_repair_chance' in option:
                                read_repair_chance = option.split(" = ")[1]
                            elif 'speculative_retry' in option:
                                speculative_retry = option.split(" = ")[1]

            newRow = [table_name ,table_columns,table_order,caching,comment,compaction,compression,crc_check_chance,dclocal_read_repair_chance,default_time_to_live,gc_grace_seconds,max_index_interval,memtable_flush_period_in_ms,min_index_interval,read_repair_chance,speculative_retry]
            table_info = np.vstack([table_info, newRow])

    logging.debug('GET TABLE PROPERTIES: Finalizing function.')
                    
def export_to_excel():
    global table_info

    # Export CSV
    cwd = os.getcwd()
    date = datetime. now(). strftime("%Y%m%d%I%M%S%p")
    
    # Exporting keyspace matrix
    filename = "tables_"+date+".xlsx"
    outputKeyspaceFile = os.path.join(cwd, filename)
    pd.DataFrame(table_info).to_excel(outputKeyspaceFile, header=False, index=False)

def main():
    # Command line arguments parser
    parser = argparse.ArgumentParser()
    parser.add_argument("inputFile", help="")
    parser = argparse.ArgumentParser()
    parser.add_argument("inputFile", help="Path to a file contaning the cql schema")
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

        get_table_properties(content)
        export_to_excel()        

if __name__ == "__main__":
    main()

