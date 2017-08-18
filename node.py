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

from component.cpu import Cpu
from component.disk import Disk
from component.mem import Mem
from component.network import Network
from component.perf import Perf
from utils.commonOps import get_file_names


class Node:
    node_attrib = ['cpustat', 'memstat', 'iostat', 'netstat', 'perfout']
    node_attrib_avg = dict.fromkeys(node_attrib, [])
    node_attrib_all = dict.fromkeys(node_attrib, [])

    def __init__(self, file_path):
        self.file_path = file_path
        print self.file_path

    def node_exist_attrib(self):
        return get_file_names(self.file_path)

    def get_avg_cpu(self):
        cpu_file = self.file_path + os.sep + self.node_attrib[0]
        if os.path.isfile(cpu_file):
            return Cpu(cpu_file).get_data()[0]
        else:
            print 'Warning: file: s% does not exist, will ignore it' % cpu_file

    def get_avg_mem(self):
        mem_file = self.file_path + os.sep + self.node_attrib[1]
        if os.path.isfile(mem_file):
            return Mem(mem_file).get_data()[0]
        else:
            print 'Warning: file: s% does not exist, will ignore it' % mem_file

    def get_avg_disk(self):
        disk_file = self.file_path + os.sep + self.node_attrib[2]
        if os.path.isfile(disk_file):
            return Mem(disk_file).get_data()[0]
        else:
            print 'Warning: file: s% does not exist, will ignore it' % disk_file

    def get_avg_attrib(self, attrib):
        attrib_file = self.file_path + os.sep + attrib
        if attrib == 'cpustat':
            if os.path.isfile(attrib_file):
                return Cpu(attrib_file).get_data()[0]
        elif attrib == 'memstat':
            if os.path.isfile(attrib_file):
                return Mem(attrib_file).get_data()[0]
        elif attrib == 'iostat':
            if os.path.isfile(attrib_file):
                return Disk(attrib_file).get_data()[0]
        elif attrib == 'netstat':
            if os.path.isfile(attrib_file):
                return Network(attrib_file).get_data()[0]
        elif attrib == 'perfout':
            if os.path.isfile(attrib_file):
                return Perf(attrib_file).get_data()[0]
        else:
            print 'node does not have attribute %s' % attrib
