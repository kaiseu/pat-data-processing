#!/usr/bin/python
# encoding: utf-8
"""

@author: xuk1
@license: (C) Copyright 2013-2017
@contact: kai.a.xu@intel.com
@file: processing.py
@time: 8/23/2017 13:57
@desc: 

"""

from cluster import Cluster
from bb_parse import BBPhase
import time, sys


def env_check():
    py_version = sys.version_info
    if py_version[:2] >= (2, 7):
        print "---- You currently have Python " + sys.version
    else:
        print "---- Error, You need python 2.7.x+ and currently you have " + sys.version + 'exiting now...'
        exit(-1)

    try:
        import numpy, pandas
    except ImportError:
        print '---- missing dependency: numpy or pandas, please install first'
        exit(-1)

    print '---- You have all required dependencies, starting to process'


if __name__ == '__main__':
    env_check()
    pat_path = 'C:\\Users\\xuk1\\PycharmProjects\\tmp_data\\pat_cdh511_HoS_27workers_2699v4_72vcores_PCIe_30T_4S_r1'
    bb_log_path = 'C:\\Users\\xuk1\\PycharmProjects\\tmp_data\\logs_cdh511_HoS_27workers_2699v4_72vcores_PCIe_30T_4S_r1'
    cluster = Cluster(pat_path)
    bb_phase = BBPhase(bb_log_path)
    begin = time.time()
    start, end = bb_phase.get_stamp_by_phase('BENCHMARK')
    print 'start timestamp is: {0} \nend timestamp is: {1} \n'.format(start, end)
    cluster.print_cluster_avg_by_time(start, end)
    stop = time.time()
    print 'elapsed time: {0}'.format(stop - begin)
