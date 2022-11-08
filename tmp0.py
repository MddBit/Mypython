#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov  7 20:38:08 2022

@author: mdd
"""

import pandas as pd
import csv

path = '/Users/mdd/Documents/src/spyder/table_metadata/tmp2.csv'

f = open(path, 'r', encoding='big5', errors='ignore')
reader = csv.reader(_.replace('\x00', '') for _ in f)
next (reader)
q1 = pd.DataFrame(reader, dtype=str)

