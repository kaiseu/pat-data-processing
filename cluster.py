#!/usr/bin/python
# encoding: utf-8
"""

@author: xuk1
@license: (C) Copyright 2013-2017
@contact: kai.a.xu@intel.com
@file: cluster.py
@time: 8/15/2017 10:38
@desc: 

"""

import numpy as np

from component.cpu import Cpu
from component.mem import Mem
from node import Node
from utils.commonOps import get_paths


class Cluster(Node):
    def __init__(self, pat_path):
        self.pat_path = pat_path
        self.nodes = self.get_nodes()
        self.cluster_attri = Node.node_attri
        self.cluster_attri_avg = dict.fromkeys(self.cluster_attri, [])

    def get_nodes(self):
        return get_paths(self.pat_path)

    def get_avg(self):
        print len(Cpu.used_col)
        cpu_sum = np.zeros((1, len(Cpu.used_col)))
        mem_sum = np.zeros((1, len(Mem.used_col)))
        attrib_sum = []
        for node in self.nodes:
            tmp_node = Node(node)
            for attrib in tmp_node.node_exist_attrib():
                attrib_sum += tmp_node.get_avg_attrib(attrib)
            cpu_sum += tmp_node.get_avg_cpu()
            mem_sum += tmp_node.get_avg_mem()
        print cpu_sum
        print mem_sum
        cpu_sum /= cpu_sum.shape[1]
        mem_sum /= mem_sum.shape[1]
        print cpu_sum
        print mem_sum


if __name__ == '__main__':
    pat_path = 'C:\\Users\\xuk1\\PycharmProjects\\tmp_data\\pat_spark163_dynamic_disable_1TB_r1\\instruments\\'
    cluster = Cluster(pat_path)
    cluster.get_avg()
