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
    """
    Node network attribute, phasing network data from original PAT file
    """
    used_col = ['HostName', 'TimeStamp', 'IFACE', 'rxkB/s', 'txkB/s']

    def __init__(self, file_path):
        self.file_path = file_path

    def get_data(self):
        """
        get average value of this attribute and all value
        :return: average value, all value
        """
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
                    ccc = ccc.set_index('TimeStamp')
                    nic_all[name_nics[num]] = ccc
        # print nic_average.transpose()
        all_average = nic_average.transpose().mean(axis=0)  # average of all nics
        return all_average, nic_all

    # def get_data_by_time(self, start, end):
    #     """
    #     get average value of this attribute and all value within the start and end timestamp
    #     :param start: start timestamp
    #     :param end: end timestamp
    #     :return: average value, all value within the given timestamp
    #     """
    #     df = pd.read_csv(self.file_path, delim_whitespace=True, usecols=self.used_col, header=0)
    #     all_row = list(df['TimeStamp'].str.contains('TimeStamp'))
    #     num_nics = all_row.index(True)  # num of NICs
    #     name_nics = df['IFACE'].loc[0:num_nics - 1]
    #     df = df[df['TimeStamp'] != 'TimeStamp']  # drop rows from df that contains 'TimeStamp'
    #     df.pop('IFACE')
    #
    #     time_stamp = df['TimeStamp'].loc[0:len(all_row):num_nics].reset_index(drop=True).astype('int64')  # time
    #     nic_average = pd.DataFrame()
    #     nic_all = {}
    #     # mask = (time_stamp >= int(start)) & (time_stamp <= int(end))
    #     for num in range(num_nics):
    #         nic_data = df.iloc[num:len(all_row):num_nics].reset_index(drop=True)
    #         # bbb = nic_data[mask]
    #         pd.to_datetime(nic_data['TimeStamp'], unit='s')
    #         bbb = nic_data.set_index('TimeStamp').loc[str(start): str(end)]
    #         ccc_avg = bbb[self.used_col[2:]].astype('float32').mean(axis=0)
    #         # ignore local lookback, ignore the NICs whose average value all smaller than 100
    #         if (name_nics[num] != 'lo') & all(x > 100 for x in ccc_avg):
    #             nic_average[name_nics[num]] = ccc_avg  # average of each nic
    #             nic_all[name_nics[num]] = bbb
    #     # print nic_average.transpose()
    #     all_average = nic_average.mean(axis=1)  # average of all nics
    #     return all_average, nic_all

    def get_data_by_time(self, start, end):
        """
        get average value of this attribute and all raw data within the start and end timestamp.
        if start and end all equal to 0 will calculate all the data.
        :param start: list of start timestamp
        :param end: list of end timestamp, should be the same length of start
        :return: dict that contains avg value and all raw data of all the timestamp pair
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
        df = df.set_index('TimeStamp')
        avg = {}
        raw_all = {}
        if start == end == 0:  # calc all the data
            raw_all[0] = df
            nic_avg = pd.DataFrame()
            for num in range(num_nics):  # processing each nic
                nic_data = df.iloc[num:len(all_row):num_nics]
                tmp = nic_data.iloc[:, 2:].astype('float32').mean(axis=0)  # average of each nics
                nic_avg = nic_avg.append(tmp, ignore_index=True)
            avg[0] = nic_avg.mean(axis=0)  # average value
            return avg, raw_all

        for i in range(len(start)):  # calc the data within the pair of time period
            raw_all[i] = df.loc[str(start[i]): str(end[i])]
            nic_avg = pd.DataFrame()
            for num in range(num_nics):  # processing each nic
                    nic_data = raw_all[i].iloc[num:len(all_row):num_nics]
                    tmp = nic_data.iloc[:, 2:].astype('float32').mean(axis=0)  # average of each nics
                    nic_avg = nic_avg.append(tmp, ignore_index=True)
            avg[i] = nic_avg.mean(axis=0)  # average value
        return avg, raw_all

if __name__ == '__main__':
    network = Network('C:\\Users\\xuk1\PycharmProjects\\tmp_data\pat_spark163_1TB_r1\\instruments\\hsx-node1\\netstat')
    avg, all_raw = network.get_data_by_time([1487687161, 1487687176], [1487687170, 1487687185])
    print avg
    print all_raw