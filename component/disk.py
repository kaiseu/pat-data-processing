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
        bbb = df[self.used_col[2:]].astype('float32')  # raw data

        time_stamp = df['TimeStamp'].loc[0:len(all_row):num_disks].reset_index(drop=True)  # time
        disk_avg = pd.DataFrame()  # save avg result of each disk
        disk_all = {}  # save raw data of each disk
        for num in range(num_disks):  # processing each disk
            ccc = bbb.iloc[num:len(all_row):num_disks].reset_index(drop=True)  # every $num_disks is for the same disk
            disk_avg[name_disks[num]] = ccc.mean(axis=0)  # average of each disks
            # print disk_avg

            ccc.insert(0, 'TimeStamp', time_stamp)  # add timestamp to the data frame
            # print ccc
            disk_all[name_disks[num]] = ccc  # save all raw data
        # the name of disk whose value is smallest among all disks, which is considered as os drive
        # if the node has disks mounted but unused, this disk will be regarded as the smallest,
        # so no recommended mount unused disk to the nodes
        os_disk = disk_avg.idxmin(axis=1).value_counts().idxmax()
        print 'Warning: disk: {0} was rarely used, which is regarded as os drive, will ignore it.'.format(os_disk)
        disk_avg.pop(os_disk)  # ignore os drive when calculate sum value
        disk_avg_sum = disk_avg.sum(axis=1)  # the sum of all the averaged disks
        return disk_avg_sum, disk_all


if __name__ == '__main__':
    pat_path = 'C:\\Users\\xuk1\\PycharmProjects\\tmp_data\\pat_cdh511_HoS_27workers_2699v4_72vcores_PCIe_30T_4S_r1\\instruments\\bd20\\iostat'
    disk_all = Disk(pat_path).get_data()

    # print disk_all

