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
for dwd in list(dwd_lst['etl_task_name']):
    f = open(path2 + dwd + '.xml')
    ods_lst = re.findall('\w+___\w+|flink_source\.\w+|gzlc_real\.\w+', f.read())
    f.close()
    
    for ods in set(ods_lst):
        dwd_ods.append([dwd, ods, ods_lst.count(ods)]) # dwd任务，使用的ods表，使用次数

dwd_ods = pd.DataFrame(dwd_ods, columns=['dwd_task_name', 'used_ods_table', 'used_cnt'])
dwd_ods.to_excel(path3, index=False)

