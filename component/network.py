#!/usr/bin/python
# encoding: utf-8
"""

@author: xuk1
@license: (C) Copyright 2013-2017
@contact: kai.a.xu@intel.com
@file: network.py
@time: 8/17/2017 12:55
@desc: 

"""

import pandas as pd

from component.base import CommonBase


class Network(CommonBase):
    used_col = ['TimeStamp', 'IFACE', 'rxkB/s', 'txkB/s']

    def __init__(self, file_path):
        self.file_path = file_path

    def get_data(self):
        df = pd.read_csv(self.file_path, delim_whitespace=True, usecols=self.used_col, header=0)

        all_row = list(df['TimeStamp'].str.contains('TimeStamp'))
        num_nics = all_row.index(True)
        name_nics = df['IFACE'].loc[0:num_nics - 1]

        df = df[df['TimeStamp'] != 'TimeStamp']  # drop rows from df that contains 'TimeStamp'
        bbb = df[['rxkB/s', 'txkB/s']].apply(pd.to_numeric)  # raw data

        time_stamp = df['TimeStamp'].loc[0:len(all_row):num_nics].reset_index(drop=True)  # time
        nic_average = {}
        nic_all = {}

        for num in range(num_nics):
            ccc = bbb.iloc[num:len(all_row):num_nics].reset_index(drop=True)
            nic_average[name_nics[num]] = ccc.mean(axis=0)
            ccc['TimeStamp'] = time_stamp
            nic_all[name_nics[num]] = ccc
            # print ccc
        return nic_average, nic_all


if __name__ == '__main__':
    pat_path = 'C:\\Users\\xuk1\\PycharmProjects\\tmp_data\\pat_spark163_dynamic_disable_1TB_r1\\instruments\\hsx-node10\\netstat'
    Network(pat_path).get_data()
