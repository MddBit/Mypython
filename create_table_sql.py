#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov  7 17:19:39 2022

@author: mdd
"""

import os
import re
import pandas as pd


def default_value(col_type):
    if col_type in ('int', 'bigint'):
        return '0'
    elif col_type == 'string':
        return "''"
    elif col_type == 'double':
        return '0'
    elif col_type == 'timestamp':
        return 'now()'
    elif re.search('decimal', col_type):
        return '0'
    elif col_type == 'float':
        return '0'
    else:
        return 'error_check'


path = '/Users/mdd/Documents/src/spyder/table_metadata/'
table_metadatas = os.listdir(path)[1:]

all_sql_str = ''
for table in table_metadatas:
    
    table_metadata = pd.read_excel(path + table)

    col_min_index = 2
    col_max_index = (table_metadata[table_metadata['name'] ==
                            '# Detailed Table Information'].index.tolist()[0] - 1)
    
    table_index = (table_metadata[table_metadata['type'] ==
                            'kudu.table_name'].index.tolist()[0])
    table_name = table_metadata['comment'][table_index]
    
    
    sql_str = "CREATE TABLE mysql_source." + table_name +" (\n"
    for i in range(col_min_index, col_max_index):
        col_str = (table_metadata['name'][i] + " " + table_metadata['type'][i] +
                       " not null default ")
        default_val = default_value(table_metadata['type'][i])
        col_str = col_str + default_val + " comment ''"
        if i == col_min_index:
            col_str = "    " + col_str + "\n"
        else:
            col_str = "    ," + col_str + "\n"
        
        sql_str += col_str
    
    sql_str += "    ,PRIMARY KEY (" + table_metadata['name'][col_min_index] + ')\n'
    sql_str += ") PARTITION BY HASH (" + table_metadata['name'][col_min_index] + ')\n'
    sql_str += "PARTITIONS 8 STORED AS KUDU TBLPROPERTIES ('kudu.master_addresses'='{KUDU_MASTER_ADDRESSES}')"
    
    all_sql_str = all_sql_str + sql_str + "\n\n"
    

