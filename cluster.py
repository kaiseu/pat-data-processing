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
from datetime import datetime

import pandas as pd

from node import Node
from utils.commonOps import get_paths


class Cluster(Node):
    """
    Each PAT data and/or TPCx-BB log corresponding to a Cluster, Cluster consists of multiple Nodes
    """

    def __init__(self, pat_path):
        """
        Each cluster should have a corresponding PAT data file, given the file initially will get all 
        the nodes in the cluster and attributes of the node
        :param pat_path: PAT file path
        """
        self.pat_path = pat_path + os.sep + 'instruments'
        self.nodes = self.get_nodes()
        self.attrib = Node(self.nodes[0]).node_exist_attrib()

    def get_nodes(self):
        """
        Get all the nodes' PAT file path in this cluster
        :return: all nodes' PAT file path
        """
        if os.path.exists(self.pat_path):
            return get_paths(self.pat_path)
        else:
            print 'Path: {0} does not exist, will exit...'.format(self.pat_path)
            exit(-1)

    def get_cluster_avg(self):
        """
        Get average value of each attribute of all the nodes in the cluster
        :return: average attribute
        """
        attrib_avg = dict()
        for attrib in self.attrib:
            tmp = [(Node(node).get_avg_attrib(attrib)) for node in self.nodes]
            attrib_all = pd.concat(tmp, axis=1).transpose()
            attrib_avg[attrib] = pd.DataFrame(attrib_all.mean()).transpose()
        return attrib_avg

    def get_cluster_avg_by_time(self, start, end):
        """
        Get average value of each attribute of all the nodes in the cluster within a given time period
        :param start: start timestamp
        :param end: end timestamp
        :return: average attribute
        """
        attrib_avg = dict()
        for attrib in self.attrib:
            tmp = [(Node(node).get_avg_attrib_by_time(attrib, start, end)) for node in self.nodes]
            attrib_all = pd.concat(tmp, axis=1).transpose()
            attrib_avg[attrib] = pd.DataFrame(attrib_all.mean()).transpose()
        return attrib_avg

    def print_cluster_avg(self):
        """
        Print all the average attributes
        :return: None
        """
        attrib_avg = self.get_cluster_avg()
        for key in attrib_avg.keys():
            print 'All nodes average {0} utilization: \n {1} \n'.format(key, attrib_avg.get(key).to_string(index=False))

    def print_cluster_avg_by_time(self, start, end, *comment):
        """
        Print all the average attributes within a given time period
        :param start: start timestamp
        :param end: end timestamp
        :param comment: Optional, for printing phase name
        :return: None
        """
        attrib_avg = self.get_cluster_avg_by_time(start, end)
        start_time = datetime.fromtimestamp(start).strftime('%Y-%m-%d %H:%M:%S')
        end_time = datetime.fromtimestamp(end).strftime('%Y-%m-%d %H:%M:%S')
        if not comment:
            print '\nAll nodes average utilization between {0} and {1}'.format(start_time, end_time)
        else:
            print '\nAll nodes average utilization for phase {0} between {1} and {2}:'.format(comment[0], start_time,
                                                                                              end_time)
        for key in attrib_avg.keys():
            print 'Average {0} utilization: \n {1} \n'.format(key, attrib_avg.get(key).to_string(index=False))


if __name__ == '__main__':
    """
    test only
    """
    pat_path = 'C:\\Users\\xuk1\\PycharmProjects\\tmp_data\\pat_spark163_1TB_r1'
    cluster = Cluster(pat_path)
    start = time.time()
    cluster.print_cluster_avg_by_time(1487687766, 1487693339)
    end = time.time()
    print 'Processing elapsed time: {0}'.format(end - start)
