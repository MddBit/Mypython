#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Aug  1 14:57:12 2022

@author: mdd
"""
import re
import pandas as pd


dwd_ods = {}
used_ods = set()
ods = pd.read_excel('/Users/mdd/Documents/src/spyder/ods.xlsx', sheet_name='ods')
dwd = pd.read_excel('/Users/mdd/Documents/src/spyder/ods.xlsx', sheet_name='dwd')
path = '/Users/mdd/Documents/src/spyder/etl_task/%s.xml'
for d in dwd['task_name']:
    num = 0
    file = path % d
    file_object = open(file)
    file_content = file_object.read().lower()
    for o in ods['all_tables']:
        if re.search(o, file_content):
            used_ods.add(o)
            num += 1
    dwd_ods[d] = num
    print(d, num)
            


used_ods = pd.DataFrame(used_ods)
used_ods.to_excel('tmp.xlsx')