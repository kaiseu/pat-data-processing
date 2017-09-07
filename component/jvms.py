#!/usr/bin/python
# encoding: utf-8
"""

@author: xuk1
@license: (C) Copyright 2013-2017
@contact: kai.a.xu@intel.com
@file: jvms.py
@time: 8/21/2017 16:47
@desc: 

"""

import numpy as np
import pandas as pd

from component.base import CommonBase


class Jvms(CommonBase):
    """
    Node JVMS attribute, not implement yet
    """

    def __init__(self):
        pass

    def get_data_by_time(self, start, end):
        return [pd.DataFrame(np.zeros((3, 3)))], pd.DataFrame(np.zeros((3, 3)))
