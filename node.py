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

from utils.commonOps import get_file_names
from component.factory import AttribFactory


class Node:
    def __init__(self, file_path):
        self.file_path = file_path
        # print self.file_path

    def node_exist_attrib(self):
        file_names = get_file_names(self.file_path)
        exist_attrib = []
        for names in file_names:
            exist_attrib.append(AttribFactory().get_attrib(names))
        return exist_attrib

    def get_avg_attrib(self, attrib):
        if attrib.lower() in AttribFactory.node_attrib.keys():
            attrib_file = self.file_path + os.sep + AttribFactory.node_attrib[attrib.lower()]
            if os.path.isfile(attrib_file):
                # print attrib_file
                return AttribFactory.create_attrib(attrib, attrib_file).get_data()[0]
            else:
                print 'node does not have attribute {0}'.format(attrib)
                exit(-1)
        else:
            print 'Node does not have attrib: {0} defined, defined attributions are: {1}, will exit...'\
                .format(attrib, AttribFactory.node_attrib.keys())
            exit(-1)

    def get_avg_attrib_by_time(self, attrib, start, end):
        if attrib.lower() in AttribFactory.node_attrib.keys():
            attrib_file = self.file_path + os.sep + AttribFactory.node_attrib[attrib.lower()]
            if os.path.isfile(attrib_file):
                # print attrib_file
                return AttribFactory.create_attrib(attrib, attrib_file).get_data_by_time(start, end)[0]
            else:
                print 'node does not have attribute {0}'.format(attrib)
                exit(-1)
        else:
            print 'Node does not have attrib: {0} defined, defined attributions are: {1}, will exit...'\
                .format(attrib, AttribFactory.node_attrib.keys())
            exit(-1)

    def get_node_avg(self):
        attrib_sum = []
        for attrib in self.node_exist_attrib():
            attrib_sum.append(self.get_avg_attrib(attrib))
        return attrib_sum

if __name__ == '__main__':
    pat_path = 'C:\\Users\\xuk1\\PycharmProjects\\tmp_data\\pat_cdh511_HoS_27workers_2699v4_72vcores_PCIe_30T_4S_r1\\instruments\\bd20'
    node = Node(pat_path)
    print node.get_avg_attrib('mem')
