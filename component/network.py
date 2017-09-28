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

import numpy as np
import pandas as pd

from component.base import CommonBase


class Network(CommonBase):
    """
    Node network attribute, phasing network data from original PAT file
    """
    used_col = ['HostName', 'TimeStamp', 'IFACE', 'rxkB/s', 'txkB/s']
    converter = {col: np.float32 for col in used_col[3:]}

    def __init__(self, file_path):
        self.file_path = file_path

    def get_data_by_time(self, start, end):
        """
        get average value of this attribute and all raw data within the start and end timestamp.
        if start and end all equal to [0] will calculate all the data.
        :param start: list of start timestamp
        :param end: list of end timestamp, should be the same length of start
        :return: dict that contains avg value of all the timestamp pair and all raw data
        """
        df = pd.read_csv(self.file_path, delim_whitespace=True, usecols=self.used_col, header=0)
        df = df[df['IFACE'] != 'lo'].reset_index(drop=True)
        all_row = list(df['TimeStamp'].str.contains('TimeStamp'))
        num_nics = all_row.index(True)  # num of NICs
        name_nics = df['IFACE'].loc[0:num_nics - 1]
        for num in range(num_nics):
            if name_nics[num] == 'lo':  # drop local nic
                name_nics = name_nics.drop(num)
                break
        df = df[df['TimeStamp'] != 'TimeStamp']  # drop rows from df that contains 'TimeStamp'
        pd.to_datetime(df['TimeStamp'], unit='s')
        df = df.set_index('TimeStamp').astype(self.converter)
        avg = []
        if start[0] == end[0] == 0:  # calc all the data
            nic_avg = pd.DataFrame()
            for num in range(num_nics):  # processing each nic
                nic_data = df.iloc[num:len(all_row):num_nics]
                tmp = nic_data.iloc[:, 2:].mean(axis=0)  # average of each NICs
                nic_avg = nic_avg.append(tmp, ignore_index=True)
            avg.append(nic_avg.sum(axis=0))  # sum of all the averaged NICs
            if len(start) == 1:
                return avg, df
            else:
                for i in range(1, len(start)):  # calc the data within the pair of time period
                    # raw_all.append(df.loc[str(start[i]): str(end[i])])
                    nic_avg = pd.DataFrame()
                    for num in range(num_nics):  # processing each nic
                        nic_data = df.loc[str(start[i]): str(end[i])].iloc[num:len(all_row):num_nics]
                        tmp = nic_data.iloc[:, 2:].mean(axis=0)  # average of each NICs
                        nic_avg = nic_avg.append(tmp, ignore_index=True)
                    avg.append(nic_avg.sum(axis=0))  # sum of all the averaged NICs
                return avg, df

        for i in range(len(start)):  # calc the data within the pair of time period
            # raw_all.append(df.loc[str(start[i]): str(end[i])])
            nic_avg = pd.DataFrame()
            for num in range(num_nics):  # processing each nic
                nic_data = df.loc[str(start[i]): str(end[i])].iloc[num:len(all_row):num_nics]
                tmp = nic_data.iloc[:, 2:].mean(axis=0)  # average of each nics
                nic_avg = nic_avg.append(tmp, ignore_index=True)
            avg.append(nic_avg.sum(axis=0))  # sum of all the averaged NICs
        return avg, df
