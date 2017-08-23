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

import os

from node import Node
from utils.commonOps import get_paths
import numpy as np
from component.factory import AttribFactory
import multiprocessing as mp
from multiprocessing import Pool
import time


class Cluster(Node):
    def __init__(self, pat_path):
        self.pat_path = pat_path
        self.nodes = self.get_nodes()
        self.attrib = Node(self.nodes[0]).node_exist_attrib()

    def get_nodes(self):
        if os.path.exists(self.pat_path):
            return get_paths(self.pat_path)
        else:
            print 'Path: {0} does not exist, will exit...'.format(self.pat_path)
            exit(-1)

    # def get_cluster_avg(self):
    #     attrib_sum = []
    #     for node in self.nodes:
    #         tmp_node = Node(node)
    #         tmp_sum = {}
    #         for attrib in tmp_node.node_exist_attrib():
    #             tmp_sum[attrib] = tmp_node.get_avg_attrib(attrib)
    #         attrib_sum.append(tmp_sum)
    #     print attrib_sum

    def get_cluster_avg(self):
        attrib_avg = {}
        for attrib in self.attrib:
            attrib_avg[attrib] = np.sum([Node(node).get_avg_attrib(attrib) for node in self.nodes], axis=0) / len(
                self.nodes)
            print 'average {0} utilization: \n {1}'.format(attrib, attrib_avg[attrib])
        print attrib_avg

        # def get_cluster_avg(self):
        #     mp.freeze_support()
        #     for node in self.nodes:
        #         process = mp.Process(target=Node(node).get_node_avg())
        #         process.start()

        # def get_cluster_avg(self):
        #     pool = Pool()
        #     result = [pool.apply_async(Node(node).get_node_avg()) for node in self.nodes]
        #     print result

    def print_cluster_avg(self):
        attrib_avg = self.get_avg_attrib()

if __name__ == '__main__':
    pat_path = 'C:\\Users\\xuk1\\PycharmProjects\\tmp_data\\pat_spark163_1TB_r1\\instruments\\'
    cluster = Cluster(pat_path)
    start = time.time()
    cluster.get_cluster_avg()
    end = time.time()
    print 'elapsed time: {0}'.format(end - start)
