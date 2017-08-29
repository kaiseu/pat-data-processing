#!/usr/bin/python
# encoding: utf-8
"""

@author: xuk1
@license: (C) Copyright 2013-2017
@contact: kai.a.xu@intel.com
@file: vmstat.py
@time: 8/21/2017 16:46
@desc: 

"""

from component.base import CommonBase
import pandas as pd


class Vmstat(CommonBase):
    def __init__(self, file_path):
        self.file_path = file_path
        pass

    def get_data(self):
        return pd.DataFrame(), pd.DataFrame()

    def get_data_by_time(self, start, end):
        return pd.DataFrame(), pd.DataFrame()
