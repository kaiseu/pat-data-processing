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
from datetime import datetime
from multiprocessing import Pool, Process

import pandas as pd

from component.factory import AttribFactory
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
            print('Path: {0} does not exist, will exit...'.format(self.pat_path))
            exit(-1)

    def get_cluster_data_by_time(self, start, end, save_raw=False):
        """
        Get average value of each attribute of all the nodes in the cluster
        :param start: list of start timestamp
        :param end: list of end timestamp, should be the same length of start
        :param save_raw: Boolean, whether to save all raw data
        :return: all nodes average value within the given time pair
        """
        cluster_avg = {}
        for attrib in self.attrib:
            tmp_avg = pd.DataFrame()
            tmp_all = pd.DataFrame()
            raw_path = self.pat_path + os.sep + attrib + '.csv'
            for node in self.nodes:
                tmp = Node(node).get_attrib_data_by_time(attrib, start, end)
                tmp_avg = tmp_avg.append(tmp[0])
                tmp_all = tmp_all.append(tmp[1])
            if save_raw:
                tmp_all.index = pd.to_datetime(tmp_all.index, unit='s')
                tmp_all.to_csv(raw_path, sep=',')
            avg = pd.DataFrame()
            for i in range(len(start)):
                avg = avg.append(tmp_avg.loc[i].mean(axis=0), ignore_index=True)
            cluster_avg[attrib] = avg
        return cluster_avg

    def get_node_attrib_data_by_time(self, file_path, attrib, start, end):
        """
        Get data of a given attribute within a given time period
        :param attrib: input attribute
        :param start: list of start timestamp
        :param end: list of end timestamp, should be the same length of start
        :return: dict that contains avg value and all raw data of all the timestamp pair
        """
        if attrib.lower() in AttribFactory.node_attrib.keys():
            attrib_file = file_path + os.sep + AttribFactory.node_attrib[attrib.lower()]
            if os.path.isfile(attrib_file):
                return AttribFactory.create_attrib(attrib, attrib_file).get_data_by_time(start, end)
            else:
                print('node does not have attribute {0}'.format(attrib))
                exit(-1)
        else:
            print('Node does not have attrib: {0} defined, defined attributions are: {1}, will exit...' \
                  .format(attrib, AttribFactory.node_attrib.keys()))
            exit(-1)

    def get_cluster_attrib_data(self, attrib, start, end):
        pool = Pool(4)
        pool.map(self.get_node_attrib_data_by_time())
        tmp_avg = pd.DataFrame()
        for node in self.nodes:
            p = Process(target=self.get_node_attrib_data_by_time(), args=(node, attrib, start, end))
            p.start()
        print(tmp_avg)

    def save_result(self, result, result_path):
        """
        save results to file
        :param result: dict that contains all the average result
        :param result_path: file path that intended to save result, default is in the input PAT dir
        :return: 
        """
        with open(result_path, 'a') as f:
            for key, value in result.items():
                f.write('*' * 100 + '\n')
                f.write('Average {0} utilization: \n {1} \n'
                        .format(key, value.to_string(index=False)))
                f.write('*' * 100 + '\n')

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
                print('All nodes average {0} utilization: \n {1} \n' \
                      .format(key, attrib_avg.get(key).to_string(index=False)))
            return
        num_input = len(option)
        if num_input == 1 or num_input > 3:
            print('Optional inputs must be (start_timestamp, end_timestamp) ' \
                  'or (start_timestamp, end_timestamp, phase_name)')
            exit(-1)
        attrib_avg = self.get_cluster_avg_by_time(option[0], option[1])
        start_time = datetime.fromtimestamp(option[0]).strftime('%Y-%m-%d %H:%M:%S')
        end_time = datetime.fromtimestamp(option[1]).strftime('%Y-%m-%d %H:%M:%S')

        if num_input == 2:
            print('\nAll nodes average utilization between {0} and {1}'.format(start_time, end_time))
        elif num_input == 3:
            print('\nAll nodes average utilization for phase {0} between {1} and {2}:' \
                  .format(option[2], start_time, end_time))
        for key in attrib_avg.keys():
            print('Average {0} utilization: \n {1} \n'.format(key, attrib_avg.get(key).to_string(index=False)))

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
                print('optional inputs must be (start_timestamp, end_timestamp) '
                      'or (start_timestamp, end_timestamp, phase)')
                exit(-1)
            for key in attrib_avg.keys():
                f.write('\nAverage {0} utilization: \n {1} \n'.format(key, attrib_avg.get(key).to_string(index=False)))
            print('Results have been saved to: {0}'.format(result_file))
