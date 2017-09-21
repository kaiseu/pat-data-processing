#!/usr/bin/python
# encoding: utf-8
"""

@author: xuk1
@license: (C) Copyright 2013-2017
@contact: kai.a.xu@intel.com
@file: parallelprocessing.py
@time: 9/15/2017 15:43
@desc: 

"""
import os
import time
from functools import partial
from multiprocessing import Pool

import pandas as pd

from cluster import Cluster
from component.factory import AttribFactory
from utils.commonOps import get_paths


def get_node_attrib_data_by_time(attrib, start, end, file_path):
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
            print 'node does not have attribute {0}'.format(attrib)
            exit(-1)
    else:
        print 'Node does not have attrib: {0} defined, defined attributions are: {1}, will exit...' \
            .format(attrib, AttribFactory.node_attrib.keys())
        exit(-1)


def get_cluster_attrib_data(pat_path, start, end, save_raw, attrib):
    """
    Get the avg data of a given attrib of the cluster.
    :param pat_path: pat path of raw data
    :param start: list of start timestamp
    :param end: list of end timestamp
    :param save_raw: whether to save the raw avg data
    :param attrib: attrib intended to calc, which could be cpu, disk, network, mem
    :return: data frame that contains the avg attrib data of the cluster
    """
    pat_path = pat_path + os.sep + 'instruments'
    if os.path.exists(pat_path):
        nodes = get_paths(pat_path)
    else:
        print 'Path: {0} does not exist, will exit...'.format(pat_path)
        exit(-1)

    pool = Pool(processes=None)
    func = partial(get_node_attrib_data_by_time, attrib, start, end)
    p = pool.map(func, nodes)

    tmp_avg = pd.DataFrame()
    tmp_all = pd.DataFrame()

    for i in range(len(p)):
        tmp_avg = tmp_avg.append(p[i][0])  # avg data
        if save_raw:
            tmp_all = tmp_all.append(p[i][1])  # all raw data
    if save_raw:
        raw_path = pat_path + os.sep + attrib + '.csv'
        tmp_all.index = pd.to_datetime(tmp_all.index, unit='s')
        tmp_all.to_csv(raw_path, sep=',')

    avg = pd.DataFrame()
    for i in range(len(start)):
        avg = avg.append(tmp_avg.loc[i].mean(axis=0), ignore_index=True)
    return avg


def get_cluster_data_by_time(pat_path, start, end, save_raw):
    """
    Get the avg data of all the attrib of the cluster.
    :param pat_path: pat path of raw data
    :param start: list of start timestamp
    :param end: list of end timestamp
    :param save_raw: whether to save the raw avg data
    :return: dict that contains all the avg attrib data of the cluster
    """
    cluster = Cluster(pat_path)
    cluster_avg = {}

    for attr in cluster.attrib:
        cluster_avg[attr] = get_cluster_attrib_data(pat_path, start, end, save_raw, attr)
    if 'cpu' in cluster_avg.keys():
        cluster_avg['cpu'].insert(0, '1-%idle', 100 - cluster_avg['cpu']['%idle'])  # add column '1-%idle' to the result
    return cluster_avg


if __name__ == '__main__':
    pat_path = 'C:\\Users\\xuk1\PycharmProjects\\tmp_data\pat_spark163_1TB_r1'
    tic = time.time()
    # pat_path = 'C:\\Users\\xuk1\\PycharmProjects\\tmp_data\\pat_cdh511_HoS_27workers_2699v4_72vcores_PCIe_30T_4S_r1'
    # pat_path = sys.argv[1]
    # nodes = Cluster(pat_path).nodes
    # attrib = 'cpu'
    # start = [0, 1505148392, 1505148392, 1505148392]
    # end = [0, 1505272675, 1505272675, 1505272675]
    start = [0, 1487687161, 1487687176]
    end = [0, 1487687170, 1487687185]
    # get_cluster_attrib_data(nodes, attrib, start, end)

    get_cluster_data_by_time(pat_path, start, end, False)
    toc = time.time()
    print 'Processing elapsed time: {0}'.format(toc - tic)