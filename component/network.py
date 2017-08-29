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
        num_nics = all_row.index(True)  # num of NICs
        name_nics = df['IFACE'].loc[0:num_nics - 1]
        df = df[df['TimeStamp'] != 'TimeStamp']  # drop rows from df that contains 'TimeStamp'
        bbb = df[self.used_col[2:]].astype('float32')  # raw data

        time_stamp = df['TimeStamp'].loc[0:len(all_row):num_nics].reset_index(drop=True)  # time
        nic_average = pd.DataFrame()
        nic_all = {}

        for num in range(num_nics):
            ccc = bbb.iloc[num:len(all_row):num_nics].reset_index(drop=True)
            ccc_avg = ccc.mean(axis=0)
            if name_nics[num] != 'lo':  # ignore local lookback
                if all(x > 100 for x in ccc_avg):  # ignore the NICs whose average value all smaller than 100
                    nic_average[name_nics[num]] = ccc_avg  # average of each nic
                    ccc.insert(0, 'TimeStamp', time_stamp)  # add timestamp to the data frame
                    nic_all[name_nics[num]] = ccc
        # print nic_average.transpose()
        all_average = nic_average.transpose().mean(axis=0)  # average of all nics
        return all_average, nic_all

    def get_data_by_time(self, start, end):
        """
        get average value of this attribute and all value within the start and end timestamp
        :param start: start timestamp
        :param end: end timestamp
        :return: average value, all value within the given timestamp
        """
        df = pd.read_csv(self.file_path, delim_whitespace=True, usecols=self.used_col, header=0)
        all_row = list(df['TimeStamp'].str.contains('TimeStamp'))
        num_nics = all_row.index(True)  # num of NICs
        name_nics = df['IFACE'].loc[0:num_nics - 1]
        df = df[df['TimeStamp'] != 'TimeStamp']  # drop rows from df that contains 'TimeStamp'
        df.pop('IFACE')

        time_stamp = df['TimeStamp'].loc[0:len(all_row):num_nics].reset_index(drop=True).astype('int64')  # time
        nic_average = pd.DataFrame()
        nic_all = {}
        mask = (time_stamp >= int(start)) & (time_stamp <= int(end))
        for num in range(num_nics):
            nic_data = df.iloc[num:len(all_row):num_nics].reset_index(drop=True)
            bbb = nic_data[mask]
            ccc_avg = bbb[self.used_col[2:]].astype('float32').mean(axis=0)
            # ignore local lookback, ignore the NICs whose average value all smaller than 100
            if (name_nics[num] != 'lo') & all(x > 100 for x in ccc_avg):
                    nic_average[name_nics[num]] = ccc_avg  # average of each nic
                    nic_all[name_nics[num]] = bbb
        # print nic_average.transpose()
        all_average = nic_average.mean(axis=1)  # average of all nics
        return all_average, nic_all

if __name__ == '__main__':
    # pat_path = 'C:\\Users\\xuk1\\PycharmProjects\\tmp_data\\pat_cdh511_HoS_27workers_2699v4_72vcores_PCIe_30T_4S_r1\\instruments\\bd20\\netstat'
    pat_path = 'C:\\Users\\xuk1\\PycharmProjects\\tmp_data\\pat_spark163_1TB_r1\\instruments\\hsx-node5\\netstat'
    Network(pat_path).get_data_by_time(1487687766, 1487693339)
