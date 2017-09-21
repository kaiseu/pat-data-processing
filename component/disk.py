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

import numpy as np
import pandas as pd

from component.base import CommonBase


class Disk(CommonBase):
    """
    Node iostat attribute, phasing disk data from original PAT file
    """
    used_col = ['HostName', 'TimeStamp', 'Device:', 'r/s', 'w/s', 'rkB/s', 'wkB/s', 'await', '%util']
    converter = {col: np.float32 for col in used_col[3:]}

    def __init__(self, file_path):
        self.file_path = file_path

    def get_data_by_time(self, start, end):
        """
        get average value of this attribute and all value within the start and end timestamp.
        if start and end all equal to [0] will calculate all the data.
        :param start: list of start timestamp
        :param end: list of end timestamp, should be the same length of start
        :return: dict that contains avg value of all the timestamp pair and all raw data
        """
        df = pd.read_csv(self.file_path, delim_whitespace=True, usecols=self.used_col, header=0)
        all_row = list(df['TimeStamp'].str.contains('TimeStamp'))
        # all the number of disks collected which is equal to the index of first 'True' in the list
        num_disks = all_row.index(True)
        name_disks = df['Device:'].loc[0:num_disks - 1]  # name of disks
        df = df[df['TimeStamp'] != 'TimeStamp']  # drop rows from df that contains 'TimeStamp'
        pd.to_datetime(df['TimeStamp'], unit='s')
        df = df.set_index('TimeStamp').astype(self.converter)
        avg = []
        if start[0] == end[0] == 0:  # calc all the data
            disk_avg = pd.DataFrame()
            for num in range(num_disks):  # processing each disk
                disk_data = df.iloc[num:len(all_row):num_disks].reset_index(
                    drop=True)  # every $num_disks is for the same disk
                tmp = disk_data.iloc[:, 2:].mean(axis=0)  # average of each disks
                disk_avg = disk_avg.append(tmp, ignore_index=True)
            avg.append(disk_avg.sum(axis=0))  # sum of all the averaged disks
            if len(start) == 1:  # input args: [0], [0]
                return avg, df
            else:  # input args: [0, 1487687161, 1487687176], [0, 1487687170, 1487687185]
                for i in range(1, len(start)):  # calc the data within the pair of time period
                    # raw_all.append(df.loc[str(start[i]): str(end[i])])
                    disk_avg = pd.DataFrame()
                    for num in range(num_disks):  # processing each disk
                        disk_data = df.loc[str(start[i]): str(end[i])].iloc[num:len(all_row):num_disks].reset_index(
                            drop=True)  # every $num_disks is for the same disk
                        tmp = disk_data.iloc[:, 2:].astype('float32').mean(axis=0)  # average of each disks
                        disk_avg = disk_avg.append(tmp, ignore_index=True)
                    avg.append(disk_avg.sum(axis=0))  # sum of all the averaged disks
                return avg, df
        # input args: [1487687161, 1487687176], [1487687170, 1487687185]
        for i in range(len(start)):  # calc the data within the pair of time period
            # raw_all.append(df.loc[str(start[i]): str(end[i])])
            disk_avg = pd.DataFrame()
            for num in range(num_disks):  # processing each disk
                disk_data = df.loc[str(start[i]): str(end[i])].iloc[num:len(all_row):num_disks].reset_index(
                    drop=True)  # every $num_disks is for the same disk
                tmp = disk_data.iloc[:, 2:].astype('float32').mean(axis=0)  # average of each disks
                disk_avg = disk_avg.append(tmp, ignore_index=True)
            avg.append(disk_avg.sum(axis=0))  # sum of all the averaged disks
        return avg, df


if __name__ == '__main__':
    disk = Disk('C:\\Users\\xuk1\PycharmProjects\\tmp_data\pat_spark163_1TB_r1\\instruments\\hsx-node1\\iostat')
    avg, all_raw = disk.get_data_by_time([0, 1487687161, 1487687176], [0, 1487687170, 1487687185])
    print avg
    print all_raw
