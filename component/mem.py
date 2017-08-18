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

import numpy as np
import pandas as pd

from component.base import CommonBase


class Mem(CommonBase):
    used_col = [16, 17, 19, 20]  # names = ['kbmemfree', 'kbmemused', ''kbbuffers', 'kbcached']

    def __init__(self, file_path):
        self.file_path = file_path

    def get_data(self):
        df = pd.read_csv(self.file_path, delim_whitespace=True,
                         skiprows=(lambda i: i % 2 == 0),
                         header=0, usecols=self.used_col)
        avg = np.mean(df.values, 0)
        return avg, df.values

    def used_col_num(self):
        return len(self.__used_col)
