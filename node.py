#!/usr/bin/python
# encoding: utf-8
"""

@author: xuk1
@license: (C) Copyright 2013-2017
@contact: kai.a.xu@intel.com
@file: node.py
@time: 8/15/2017 10:47
@desc: 

"""

import os

from component.factory import AttribFactory
from utils.commonOps import get_file_names


class Node:
    """
    Corresponding to a physical machine, each Node may have many attributes, nodes makes up Cluster
    """
    def __init__(self):
        pass

    def __init__(self, file_path):
        self.file_path = file_path
        self.attrib = self.node_exist_attrib()

    def node_exist_attrib(self):
        """
        Get attributes this node has
        :return: exist attributes of this node
        """
        file_names = get_file_names(self.file_path)
        exist_attrib = []
        for names in file_names:
            exist_attrib.append(AttribFactory().get_attrib(names))
        return sorted(exist_attrib)

    def get_attrib_data_by_time(self, attrib, start, end):
        """
        Get data of a given attribute within a given time period
        :param attrib: input attribute
        :param start: list of start timestamp
        :param end: list of end timestamp, should be the same length of start
        :return: dict that contains avg value and all raw data of all the timestamp pair
        """
        if attrib.lower() in AttribFactory.node_attrib.keys():
            attrib_file = self.file_path + os.sep + AttribFactory.node_attrib[attrib.lower()]
            if os.path.isfile(attrib_file):
                return AttribFactory.create_attrib(attrib, attrib_file).get_data_by_time(start, end)
            else:
                print 'node does not have attribute {0}'.format(attrib)
                exit(-1)
        else:
            print 'Node does not have attrib: {0} defined, defined attributions are: {1}, will exit...' \
                .format(attrib, AttribFactory.node_attrib.keys())
            exit(-1)

