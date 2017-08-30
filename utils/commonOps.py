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


def get_paths(pat_path):
    """
    get all PAT source data file given the PAT path
    :param pat_path: PAT source data path
    :return: list contains all source data paths
    """
    node_paths = []
    for root, dirs, files in os.walk(pat_path):
        for name in dirs:
            node_paths.append(os.path.join(root, name))
    return node_paths


def get_file_names(path):
    """
    Given a dir path, returns a list of files in this dir, not includes its child dir
    :param path: 
    :return: 
    """
    names = []
    if os.path.exists(path):
        for (dirpath, dirnames, filenames) in os.walk(path):
            names.extend(filenames)
            break
    else:
        print 'path: %s does not exit, please check.' % path
    return names
