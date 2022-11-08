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


path1 = '/Users/mdd/Documents/src/Mypython/table_metadata/' # ods元数据
path2 = '/Users/mdd/Documents/src/Mypython/all_sql_str.sql' # 结果输出，建表语句
metadata_book_list = os.listdir(path1)
metadata_book_list.remove('.DS_Store')


f = open(path2, 'a')
for book in metadata_book_list:
    
    sheet_list = pd.read_excel(path1 + book, sheet_name=None)
    
    for sheet in sheet_list:
        table_metadata = sheet_list[sheet]
        
        col_min_index = 2
        col_max_index = table_metadata[table_metadata['name'] == '# Detailed Table Information'].index.tolist()[0] - 1
        
        table_index = table_metadata[table_metadata['type'] == 'kudu.table_name'].index.tolist()[0]
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
        
        f.write(sql_str)
        f.write("\n\n")
f.close()

    
