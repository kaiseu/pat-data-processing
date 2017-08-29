#!/usr/bin/python
# encoding: utf-8
"""

@author: xuk1
@license: (C) Copyright 2013-2017
@contact: kai.a.xu@intel.com
@file: cpu.py
@time: 8/15/2017 10:50
@desc: 

"""

import numpy as np
import pandas as pd

from component import base


class Cpu(base.CommonBase):
    used_col = ['TimeStamp', '%user', '%nice', '%system', '%iowait', '%steal', '%idle']

    def __init__(self):
        pass

    def __init__(self, file_path):
        self.file_path = file_path

    def get_data(self):
        """
        get average value of this attribute and all value align with timestamp 
        :return: average value, all value
        """
        df = pd.read_csv(self.file_path, delim_whitespace=True,
                         usecols=self.used_col, header=0)
        # avg = np.mean(df.iloc[:, 1:len(self.used_col)].values, 0)
        avg = df.iloc[:, 1:len(self.used_col)].astype('float32').mean()
        return avg, df.values

    def get_data_by_time(self, start, end):
        """
        get average value of this attribute and all value within the start and end timestamp
        :param start: start timestamp
        :param end: end timestamp
        :return: average value, all value within the given timestamp
        """
        df = pd.read_csv(self.file_path, delim_whitespace=True,
                         usecols=self.used_col, header=0)
        mask = (df['TimeStamp'] >= start) & (df['TimeStamp'] <= end)
        df = df.loc[mask]
        avg = df.iloc[:, 1:len(self.used_col)].astype('float32').mean()
        return avg, df

    def used_col_num(self):
        return len(self.__used_col)


if __name__ == '__main__':
    # pat_path = 'C:\\Users\\xuk1\\PycharmProjects\\tmp_data\\pat_cdh511_HoS_27workers_2699v4_72vcores_PCIe_30T_4S_r1\\instruments\\bd21\\cpustat'
    pat_path = 'C:\\Users\\xuk1\\PycharmProjects\\tmp_data\\pat_spark163_1TB_r1\\instruments\\hsx-node6\\cpustat'
    mem = Cpu(pat_path)
    mem.get_data_by_time(1487687152, 1487687182)
