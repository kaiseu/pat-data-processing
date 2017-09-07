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
    """
    Node CPU attribute, phasing cpu data from original PAT file
    """
    used_col = ['HostName', 'TimeStamp', '%user', '%nice', '%system', '%iowait', '%steal', '%idle']
    converter = {col: np.float32 for col in used_col[2:]}

    def __init__(self):
        pass

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
        df = pd.read_csv(self.file_path, delim_whitespace=True,
                         usecols=self.used_col, header=0)
        pd.to_datetime(df['TimeStamp'], unit='s')
        df = df.set_index('TimeStamp').astype(self.converter)
        avg = []
        if start[0] == end[0] == 0:  # calc all the data
            avg.append(df.loc[:, self.used_col[2:]].mean())
            if len(start) == 1:
                return avg, df
            else:
                for i in range(1, len(start)):  # calc the data within the pair of time period
                    avg.append(df.loc[start[i]:end[i], self.used_col[2:]].mean(axis=0))
                return avg, df

        for i in range(len(start)):  # calc the data within the pair of time period
            avg.append(df.loc[start[i]:end[i], self.used_col[2:]].mean(axis=0))
        return avg, df

    def used_col_num(self):
        return len(self.__used_col)


if __name__ == '__main__':
    cpu = Cpu('C:\\Users\\xuk1\PycharmProjects\\tmp_data\pat_spark163_1TB_r1\\instruments\\hsx-node1\\cpustat')
    avg, all_raw = cpu.get_data_by_time([0, 1487687161, 1487687176], [0, 1487687170, 1487687185])
    print avg
    print all_raw
