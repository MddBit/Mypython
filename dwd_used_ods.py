#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov  7 15:03:01 2022

用正则表达式从etl脚本中匹配出使用的ods表

@author: mdd
"""

import re
import pandas as pd

path1 = "/Users/mdd/Documents/src/Mypython/etl_task.xlsx" # 当前在运行etl任务
path2 = "/Users/mdd/Documents/src/realtime_dw_cloud/etl_task/" # etl任务xml文件
path3 = "/Users/mdd/Documents/src/Mypython/dwd_used_ods.xlsx" # 结果表

dwd_lst = pd.read_excel(path1)

dwd_ods = []
for i in range(len(dwd_lst)):
    dwd = dwd_lst['etl_task_name'][i] # 获取etl任务
    table = dwd_lst['impala_table'][i] # 获取etl任务对应的impala表
    
    f = open(path2 + dwd + '.xml') # 获取etl任务的xml文件
    content = f.read()
    f.close()
    
    ods_lst = re.findall('\w+___\w+|flink_source\.\w+|gzlc_real\.\w+|hive_source\.\w+|realtime_dwb\.\w+|realtime_dwd\.\w+|realtime_flink\.\w+|realtime_dim\.\w+|realtime_bak\.\w+', content, re.I|re.A) # 获取etl任务用到的ods表
    comment = re.findall('--.+', content) # 获取注释掉的代码
    
    del_table = [] # 获取注释掉的代码中的ods表
    for s in comment:
        del_lst = re.findall('\w+___\w+|flink_source\.\w+|gzlc_real\.\w+|hive_source\.\w+|realtime_dwb\.\w+|realtime_dwd\.\w+|realtime_flink\.\w+|realtime_dim\.\w+|realtime_bak\.\w+', s, re.I|re.A)
        del_table.extend(del_lst)

    for t in del_table:
        ods_lst.remove(t) # 移除注释掉的代码包含的ods表
    
    
    
    for ods in set(ods_lst):
        if re.search('\w+___', ods): # 发现有flink_source表，名称中也有___
            ods_table = 'mysql_source.' + ods
        else:
            ods_table = ods
        dwd_ods.append([dwd, table, ods_table, ods_lst.count(ods)]) # dwd任务，使用的ods表，使用次数

dwd_ods = pd.DataFrame(dwd_ods, columns=['dwd_task_name', 'impala_table', 'used_ods_table', 'used_cnt'])
dwd_ods = dwd_ods[dwd_ods['impala_table']!=dwd_ods['used_ods_table']] # 剔除自身
dwd_ods.to_excel(path3, index=False)

