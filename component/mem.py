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
    used_col = [1, 13, 14, 16, 17]  # names = ['TimeStamp', 'kbmemfree', 'kbmemused', 'kbbuffers', 'kbcached']
    names = ['TimeStamp', 'kbmemfree', 'kbmemused', 'kbbuffers', 'kbcached']

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
        avg = df.iloc[:, 1:len(self.used_col)].mean(axis=0)
        return avg, df

    def get_data_by_time(self, start, end):
        """
        get average value of this attribute and all value within the start and end timestamp
        :param start: start timestamp
        :param end: end timestamp
        :return: average value, all value within the given timestamp
        """
        # df = pd.read_csv(self.file_path, delim_whitespace=True,
        #                  skiprows=(lambda i: i % 2 == 0), usecols=self.used_col, names=self.names)
        df = pd.read_csv(self.file_path, delim_whitespace=True,
                        usecols=self.used_col, names=self.names, header=0)
        df = df.loc[0::2].astype('int64')  # read every two rows
        mask = (df['TimeStamp'] >= int(start)) & (df['TimeStamp'] <= int(end))
        df = df.loc[mask].reset_index(drop=True)
        avg = df.iloc[:, 1:len(self.used_col)].mean(axis=0)
        return avg, df

    def used_col_num(self):
        return len(self.__used_col)


if __name__ == '__main__':
    pat_path = '/home/xuk1/Documents/BigBench_results/Sugon/intel/HoS/27-workers/pat_cdh511_HoS_27workers_2699v4_72vcores_PCIe_30T_5S_r3/instruments/bd20/memstat'
    # mem = Mem(pat_path).get_data_by_time(1503046872, 1503165891)
    mem = Mem(pat_path).get_data()
    print mem