#!/usr/bin/python
# encoding: utf-8
"""

@author: xuk1
@license: (C) Copyright 2013-2017
@contact: kai.a.xu@intel.com
@file: mem.py
@time: 8/15/2017 10:50
@desc: 

"""

import pandas as pd

from component.base import CommonBase


class Mem(CommonBase):
    """
    Node memory attribute, phasing memory data from original PAT file
    """
    used_col = [0, 1, 13, 14, 16, 17]  # names = ['TimeStamp', 'kbmemfree', 'kbmemused', 'kbbuffers', 'kbcached']
    names = ['HostName', 'TimeStamp', 'kbmemfree', 'kbmemused', 'kbbuffers', 'kbcached']

    def __init__(self, file_path):
        self.file_path = file_path

    def get_data(self):
        """
        get average value of this attribute and all value align with timestamp 
        :return: average value, all value
        """
        # df = pd.read_csv(self.file_path, delim_whitespace=True,
        #                  skiprows=(lambda i: i % 2 == 0), usecols=self.used_col, names=self.names)
        df = pd.read_csv(self.file_path, delim_whitespace=True,
                         usecols=self.used_col, names=self.names, header=0)
        df = df.loc[0::2].astype('int64')  # read every two rows
        pd.to_datetime(df['TimeStamp'], unit='s')
        df = df.set_index('TimeStamp')
        avg = df.iloc[:, 1:len(self.used_col)].mean(axis=0)
        return avg, df

    # def get_data_by_time(self, start, end):
    #     """
    #     get average value of this attribute and all value within the start and end timestamp
    #     :param start: start timestamp
    #     :param end: end timestamp
    #     :return: average value, all value within the given timestamp
    #     """
    #     # df = pd.read_csv(self.file_path, delim_whitespace=True,
    #     #                  skiprows=(lambda i: i % 2 == 0), usecols=self.used_col, names=self.names)
    #     df = pd.read_csv(self.file_path, delim_whitespace=True,
    #                     usecols=self.used_col, names=self.names, header=0)
    #     df = df.loc[0::2].astype('int64')  # read every two rows
    #     # mask = (df['TimeStamp'] >= int(start)) & (df['TimeStamp'] <= int(end))
    #     # df = df.loc[mask].reset_index(drop=True)
    #     avg = df.iloc[:, 1:len(self.used_col)].mean(axis=0)
    #     pd.to_datetime(df['TimeStamp'], unit='s')
    #     df = df.set_index('TimeStamp')
    #     df = df.loc[start: end]
    #     return avg, df

    def get_data_by_time(self, start, end):
        """
        get average value of this attribute and all raw data within the start and end timestamp.
        if start and end all equal to 0 will calculate all the data.
        :param start: list of start timestamp
        :param end: list of end timestamp, should be the same length of start
        :return: dict that contains avg value and all raw data of all the timestamp pair
        """
        df = pd.read_csv(self.file_path, delim_whitespace=True,
                         usecols=self.used_col, names=self.names, header=0)
        df = df.loc[0::2]  # read every two rows
        pd.to_datetime(df['TimeStamp'], unit='s')
        df = df.set_index('TimeStamp')
        avg = {}
        raw_all = {}
        if start == end == 0:  # calc all the data
            raw_all[0] = df
            avg[0] = df.iloc[:, 1:len(self.used_col)].astype('float32').mean(axis=0)
            return avg, raw_all

        for i in range(len(start)):  # calc the data within the pair of time period
            raw_all[i] = df.loc[str(start[i]): str(end[i])]
            avg[i] = raw_all[i].iloc[:, 1:len(self.used_col)].astype('float32').mean(axis=0)
        return avg, raw_all


    def used_col_num(self):
        return len(self.__used_col)

if __name__ == '__main__':
    mem = Mem('C:\\Users\\xuk1\PycharmProjects\\tmp_data\pat_spark163_1TB_r1\\instruments\\hsx-node1\\memstat')
    avg, all_raw = mem.get_data_by_time(0,0)
    print avg
    print all_raw