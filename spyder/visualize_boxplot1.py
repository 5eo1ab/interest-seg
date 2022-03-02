# -*- coding: utf-8 -*-
"""
Created on Wed Mar  2 22:45:55 2022

@author: hanbin5eo
"""

import os
print(os.getcwd())

import pandas as pd
from matplotlib import pyplot as plt

base_path = 'D:/hanbin5eo/work/interest-seg/data/20220302'
print(os.listdir(base_path))
map_file_nm = {
    'MY': 'MY_SELECT_imsi_no_SUM_use_amount_AS_sum_use_amount_FROM_SBXL2CK_L2_202203022226.csv',
    'MN': 'MN_SELECT_imsi_no_SUM_use_amount_AS_sum_use_amount_FROM_SBXL2CK_L2_202203022226.csv',
    'FY': 'FY_SELECT_imsi_no_SUM_use_amount_AS_sum_use_amount_FROM_SBXL2CK_L2_202203022227.csv',
    'FN': 'FN_SELECT_imsi_no_SUM_use_amount_AS_sum_use_amount_FROM_SBXL2CK_L2_202203022227.csv',
}

df_my = pd.read_csv('{}/{}'.format(base_path, map_file_nm['MY']))
df_mn = pd.read_csv('{}/{}'.format(base_path, map_file_nm['MN']))
df_fy = pd.read_csv('{}/{}'.format(base_path, map_file_nm['FY']))
df_fn = pd.read_csv('{}/{}'.format(base_path, map_file_nm['FN']))

desc = pd.DataFrame(index=df_my.describe().index)
desc['MY'] = df_my.describe()
desc['MN'] = df_mn.describe()
desc['FY'] = df_fy.describe()
desc['FN'] = df_fn.describe()
desc.boxplot()

