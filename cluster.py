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

    def get_cluster_data_by_time(self, start, end, save_raw):
        """
        Get average value of each attribute of all the nodes in the cluster
        :param option: Optional inputs must be (start_timestamp, end_timestamp), if is None will calculate the whole 
        :return: average attribute
        """
        cluster_avg = {}
        for attrib in self.attrib:
            tmp_avg = pd.DataFrame()
            tmp_all = pd.DataFrame()
            raw_path = self.pat_path + os.sep + attrib + '.h5'
            for node in self.nodes:
                tmp = Node(node).get_attrib_data_by_time(attrib, start, end)
                tmp_avg = tmp_avg.append(tmp[0])
                # tmp_all = tmp_all.append(tmp[1])
                if save_raw:
                    tmp[1].to_hdf(raw_path, attrib, format='table', append=True)
            avg = pd.DataFrame()
            for i in range(len(start)):
                avg = avg.append(tmp_avg.loc[i].mean(axis=0), ignore_index=True)
            cluster_avg[attrib] = avg

            # tmp_all.to_hdf(raw_path, 'key_to_store', table=True)
        result_path = self.pat_path + os.sep + 'results.txt'
        print cluster_avg
        self.save_result(cluster_avg, result_path)
        return cluster_avg

    def save_result(self, result, result_path):
        """
        save results to file
        :param result: dict that contains all the average result
        :param result_path: file path that intended to save result, default is in the input PAT dir
        :return: 
        """
        for key, value in result.items():
            with open(result_path, 'a') as f:
                f.write('*' * 110 + '\n')
                f.write('Average {0} utilization: \n {1} \n'
                        .format(key, value.to_string(index=False)))
                f.write('*' * 110 + '\n')

    def print_cluster_avg(self, *option):
        """
        Print all the average attributes 
        :param option: Optional inputs must be (start_timestamp, end_timestamp) 
        or (start_timestamp, end_timestamp, phase_name)
        :return: None
        """
        if not option:
            attrib_avg = self.get_cluster_avg()
            for key in attrib_avg.keys():
                print 'All nodes average {0} utilization: \n {1} \n' \
                    .format(key, attrib_avg.get(key).to_string(index=False))
            return
        num_input = len(option)
        if num_input == 1 or num_input > 3:
            print 'Optional inputs must be (start_timestamp, end_timestamp) ' \
                  'or (start_timestamp, end_timestamp, phase_name)'
            exit(-1)
        attrib_avg = self.get_cluster_avg_by_time(option[0], option[1])
        start_time = datetime.fromtimestamp(option[0]).strftime('%Y-%m-%d %H:%M:%S')
        end_time = datetime.fromtimestamp(option[1]).strftime('%Y-%m-%d %H:%M:%S')

        if num_input == 2:
            print '\nAll nodes average utilization between {0} and {1}'.format(start_time, end_time)
        elif num_input == 3:
            print '\nAll nodes average utilization for phase {0} between {1} and {2}:' \
                .format(option[2], start_time, end_time)
        for key in attrib_avg.keys():
            print 'Average {0} utilization: \n {1} \n'.format(key, attrib_avg.get(key).to_string(index=False))

    def save_avg_results(self, *option):
        """
        Save results to file
        :param option: optional inputs must be (start_timestamp, end_timestamp) 
        or (start_timestamp, end_timestamp, phase)
        :return: None
        """
        result_file = self.pat_path + os.sep + 'results.txt'
        with open(result_file, 'w') as f:
            if not option:
                attrib_avg = self.get_cluster_avg()
                for key in attrib_avg.keys():
                    f.write('All nodes average {0} utilization: \n {1} \n'
                            .format(key, attrib_avg.get(key).to_string(index=False)))
                return
            num_input = len(option)
            if num_input == 2:
                attrib_avg = self.get_cluster_avg(option[0], option[1])
                start_time = datetime.fromtimestamp(option[0]).strftime('%Y-%m-%d %H:%M:%S')
                end_time = datetime.fromtimestamp(option[1]).strftime('%Y-%m-%d %H:%M:%S')
                f.write('All nodes average utilization between {0} and {1}:\n'.format(start_time, end_time))
            elif num_input == 3:
                attrib_avg = self.get_cluster_avg(option[0], option[1])
                start_time = datetime.fromtimestamp(option[0]).strftime('%Y-%m-%d %H:%M:%S')
                end_time = datetime.fromtimestamp(option[1]).strftime('%Y-%m-%d %H:%M:%S')
                f.write('All nodes average utilization for phase {0} between {1} and {2}:\n'
                        .format(option[2], start_time, end_time))
            elif num_input == 1 or num_input > 3:
                print 'optional inputs must be (start_timestamp, end_timestamp) ' \
                      'or (start_timestamp, end_timestamp, phase)'
                exit(-1)
            for key in attrib_avg.keys():
                f.write('\nAverage {0} utilization: \n {1} \n'.format(key, attrib_avg.get(key).to_string(index=False)))
            print 'Results have been saved to: {0}'.format(result_file)


if __name__ == '__main__':
    """
    test only
    """
    pat_path = 'C:\\Users\\xuk1\\PycharmProjects\\tmp_data\\pat_cdh511_HoS_27workers_2699v4_72vcores_PCIe_30T_4S_r1'
    # pat_path = 'C:\\Users\\xuk1\PycharmProjects\\tmp_data\pat_spark163_1TB_r1'
    cluster = Cluster(pat_path)
    start = time.time()
    print cluster.get_cluster_data_by_time([0, 1487687161, 1487687176], [0, 1487687170, 1487687185], False)
    end = time.time()
    print 'Processing elapsed time: {0}'.format(end - start)

    # df = pd.read_hdf('C:\\Users\\xuk1\\PycharmProjects\\tmp_data\\pat_cdh511_HoS_27workers_2699v4_72vcores_PCIe_30T_4S_r1\\instruments\\cpu.h5', 'cpu')
    # print df
