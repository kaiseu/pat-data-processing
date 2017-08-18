#!/usr/bin/python
# encoding: utf-8
"""

@author: xuk1
@license: (C) Copyright 2013-2017
@contact: kai.a.xu@intel.com
@file: commonOps.py
@time: 8/15/2017 10:51
@desc: 

"""

import os

import numpy as np
import pandas as pd


def get_data(file_path):
    df = pd.read_csv(file_path, delimiter=' ', names=['%user', '%nice', '%system', '%iowait', '%steal', '%idle'],
                     header=0).values
    avg = np.mean(df.values, 0)
    return avg, df


def get_paths(pat_path):
    node_paths = []
    for root, dirs, files in os.walk(pat_path):
        for name in dirs:
            node_paths.append(os.path.join(root, name))
    return node_paths


"""
Given a dir path, returns a list of files in this dir, not includes its child dir
"""


def get_file_names(path):
    names = []
    if os.path.exists(path):
        for (dirpath, dirnames, filenames) in os.walk(path):
            names.extend(filenames)
            break
    else:
        print 'path: %s does not exit, please check.' % path
    return names


if __name__ == '__main__':
    file_path = 'C:\\Users\\xuk1\\PycharmProjects\\tmp_data\\pat_spark163_dynamic_disable_1TB_r1\\instruments\\'
    get_paths(file_path)
