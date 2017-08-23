#!/usr/bin/python
# encoding: utf-8
"""

@author: xuk1
@license: (C) Copyright 2013-2017
@contact: kai.a.xu@intel.com
@file: base.py
@time: 8/15/2017 10:49
@desc: 

"""


class CommonBase:
    used_col = []

    def __init__(self, file_path):
        self.file_path = file_path
        pass

    def get_data(self):
        print "base extract method"
