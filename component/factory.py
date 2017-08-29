#!/usr/bin/python
# encoding: utf-8
"""

@author: xuk1
@license: (C) Copyright 2013-2017
@contact: kai.a.xu@intel.com
@file: factory.py
@time: 8/21/2017 10:56
@desc: 

"""

from component.cpu import Cpu
from component.mem import Mem
from component.network import Network
from component.disk import Disk
from component.perf import Perf
from component.vmstat import Vmstat
from component.jvms import Jvms


class AttribFactory:
    node_attrib = {'cpu': 'cpustat', 'mem': 'memstat', 'disk': 'iostat', 'network': 'netstat', 'perf': 'perfout', 'vmstat': 'vmstat', 'jvms': 'jvms'}

    def __init__(self):
        pass

    def get_attrib(self, value):
        for attrib in self.node_attrib:
            if value == self.node_attrib[attrib]:
                return attrib

    @staticmethod
    def create_attrib(attrib, file_path):
        if attrib.lower() == 'cpu':
            return Cpu(file_path)
        elif attrib.lower() == 'mem':
            return Mem(file_path)
        elif attrib.lower() == 'network':
            return Network(file_path)
        elif attrib.lower() == 'disk':
            return Disk(file_path)
        elif attrib.lower() == 'perf':
            return Perf(file_path)
        elif attrib.lower() == 'vmstat':
            return Vmstat(file_path)
        elif attrib.lower() == 'jvms':
            return Jvms(file_path)
        else:
            print 'No attrib: {0} defined, will exit...'.format(attrib)
            exit(-1)

