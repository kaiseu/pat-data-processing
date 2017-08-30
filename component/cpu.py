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

import pandas as pd

from component import base


class Cpu(base.CommonBase):
    """
    Node CPU attribute, phasing cpu data from original PAT file
    """
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
        return avg, df

    def get_data_by_time(self, start, end):
        """
        get average value of this attribute and all value within the start and end timestamp
        :param start: start timestamp
        :param end: end timestamp
        :return: average value, all value within the given timestamp
        """
        df = pd.read_csv(self.file_path, delim_whitespace=True,
                         usecols=self.used_col, header=0)
        mask = (df['TimeStamp'] >= int(start)) & (df['TimeStamp'] <= int(end))
        df = df.loc[mask]
        avg = df.iloc[:, 1:len(self.used_col)].astype('float32').mean()
        return avg, df

    def used_col_num(self):
        return len(self.__used_col)
