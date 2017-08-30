#!/usr/bin/python
# encoding: utf-8
"""

@author: xuk1
@license: (C) Copyright 2013-2017
@contact: kai.a.xu@intel.com
@file: perf.py
@time: 8/17/2017 18:24
@desc: 

"""

import pandas as pd

from base import CommonBase


class Perf(CommonBase):
    """
    Node perf attribute, not implement yet
    """

    def __init__(self):
        pass

    def get_data(self):
        return pd.DataFrame(), pd.DataFrame()

    def get_data_by_time(self, start, end):
        return pd.DataFrame(), pd.DataFrame()
