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
import time

import pandas as pd

from node import Node
from utils.commonOps import get_paths
from multiprocessing import Pool
from datetime import datetime
import os


class Cluster(Node):
    def __init__(self, pat_path):
        self.pat_path = pat_path + os.sep + 'instruments'
        self.nodes = self.get_nodes()
        self.attrib = Node(self.nodes[0]).node_exist_attrib()

    def get_nodes(self):
        if os.path.exists(self.pat_path):
            return get_paths(self.pat_path)
        else:
            print 'Path: {0} does not exist, will exit...'.format(self.pat_path)
            exit(-1)

            # def get_cluster_avg(self):
            #     attrib_avg = {}
            #     for attrib in self.attrib:
            #         attrib_avg[attrib] = np.sum([Node(node).get_avg_attrib(attrib) for node in self.nodes], axis=0) / len(
            #             self.nodes)
            #         print 'average {0} utilization: \n {1}'.format(attrib, attrib_avg[attrib])
            #     print attrib_avg

    def get_cluster_avg(self):
        attrib_avg = dict()
        for attrib in self.attrib:
            tmp = [(Node(node).get_avg_attrib(attrib)) for node in self.nodes]
            attrib_all = pd.concat(tmp, axis=1).transpose()
            attrib_avg[attrib] = pd.DataFrame(attrib_all.mean()).transpose()
        return attrib_avg

        # def get_cluster_avg(self):
        #     mp.freeze_support()
        #     for node in self.nodes:
        #         process = mp.Process(target=Node(node).get_node_avg())
        #         process.start()

        def get_cluster_avg(self):
            pool = Pool()
            result = [pool.apply_async(Node(node).get_node_avg()) for node in self.nodes]
            print result

    def get_cluster_avg_by_time(self, start, end):
        attrib_avg = dict()
        for attrib in self.attrib:
            tmp = [(Node(node).get_avg_attrib_by_time(attrib, start, end)) for node in self.nodes]
            attrib_all = pd.concat(tmp, axis=1).transpose()
            attrib_avg[attrib] = pd.DataFrame(attrib_all.mean()).transpose()
        return attrib_avg

    def print_cluster_avg(self):
        attrib_avg = self.get_cluster_avg()
        for key in attrib_avg.keys():
            print 'All nodes average {0} utilization: \n {1} \n'.format(key, attrib_avg.get(key).to_string(index=False))

    def print_cluster_avg_by_time(self, start, end):
        attrib_avg = self.get_cluster_avg_by_time(start, end)
        start_time = datetime.fromtimestamp(start).strftime('%Y-%m-%d %H:%M:%S')
        end_time = datetime.fromtimestamp(end).strftime('%Y-%m-%d %H:%M:%S')
        print '\nAll nodes average utilization between {0} and {1}'.format(start_time, end_time)
        for key in attrib_avg.keys():
            print 'All nodes average {0} utilization: \n {1} \n'.format(key, attrib_avg.get(key).to_string(index=False))

if __name__ == '__main__':
    # pat_path = 'C:\\Users\\xuk1\\PycharmProjects\\tmp_data\\pat_cdh511_HoS_27workers_2699v4_72vcores_PCIe_30T_4S_r1\\instruments\\'
    pat_path = 'C:\\Users\\xuk1\\PycharmProjects\\tmp_data\\pat_spark163_1TB_r1'
    cluster = Cluster(pat_path)
    start = time.time()
    cluster.print_cluster_avg_by_time(1487687766, 1487693339)
    end = time.time()
    print 'Processing elapsed time: {0}'.format(end - start)
