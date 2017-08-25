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
        df = pd.read_csv(self.file_path, delim_whitespace=True,
                         names=self.used_col, header=0)
        # avg = np.mean(df.iloc[:, 1:len(self.used_col)].values, 0)
        avg = df.iloc[:, 1:len(self.used_col)].astype('float32').mean()
        return avg, df.values


    def used_col_num(self):
        return len(self.__used_col)


if __name__ == '__main__':
    cpu = Cpu("ss")
    print cpu.extract_data()
