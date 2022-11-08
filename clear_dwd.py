#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Aug  1 16:42:18 2022

@author: mdd
"""

import os
import pandas as pd


def delete_file(file):
    if os.path.exists(file):
        os.remove(file)
    else:
        print('%s不存在，删除失败' % file)
       

def main():
    path = '/Users/mdd/Documents/src/spyder/etl_task/'
    dwd = pd.read_excel('/Users/mdd/Documents/src/spyder/ods.xlsx', sheet_name='dwd')
    dwd_list = list(dwd['task_name'] + '.xml')
    
    for root, dirs, files in os.walk(path):
        file_list = files
    
    for t in file_list:
        if t not in dwd_list:
            delete_file(path + t)
            print(t)

main()