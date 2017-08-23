#!/usr/bin/python
# encoding: utf-8
"""

@author: xuk1
@license: (C) Copyright 2013-2017
@contact: kai.a.xu@intel.com
@file: disk.py
@time: 8/16/2017 11:05
@desc: 

"""

import pandas as pd

from component.base import CommonBase
import numpy as np


class Disk(CommonBase):
    used_col = ['TimeStamp', 'Device:', 'r/s', 'w/s', 'rkB/s', 'wkB/s', 'await', '%util']

    def __init__(self, file_path):
        self.file_path = file_path

    """
    phasing disk data from original PAT file
    
    """

    def get_data(self):
        df = pd.read_csv(self.file_path, delim_whitespace=True, usecols=self.used_col, header=0)
        all_row = list(df['TimeStamp'].str.contains('TimeStamp'))
        num_disks = all_row.index(
            True)  # all the number of disks collected which is equal to the index of first 'True' in the list
        name_disks = df['Device:'].loc[0:num_disks - 1]  # name of disks

        df = df[df['TimeStamp'] != 'TimeStamp']  # drop rows from df that contains 'TimeStamp'
        bbb = df[['r/s', 'w/s', 'rkB/s', 'wkB/s', 'await', '%util']].apply(pd.to_numeric)  # raw data

        time_stamp = df['TimeStamp'].loc[0:len(all_row):num_disks].reset_index(drop=True)  # time
        disk_avg = {}  # save avg result of each disk
        disk_all = {}  # save raw data of each disk
        for num in range(num_disks):  # processing each disk
            ccc = bbb.iloc[num:len(all_row):num_disks].reset_index(drop=True)  # every $num_disks is for the same disk
            disk_avg[name_disks[num]] = ccc.mean(axis=0)  # average of each disks
            # print np.average(disk_avg.values(), axis=0)
            # print disk_avg
            ccc.insert(0, 'TimeStamp', time_stamp)  # add timestamp to the data frame
            disk_all[name_disks[num]] = ccc  # save all raw data
        all_average = np.array(disk_avg.values()).mean(axis=0)  # average of all disks
        # print all_average
        return all_average, disk_all


if __name__ == '__main__':
    pat_path = 'C:\\Users\\xuk1\\PycharmProjects\\tmp_data\\pat_spark163_dynamic_disable_1TB_r1\\instruments\\hsx-node10\\iostat'
    Disk(pat_path).get_data()
