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

import sys
import time

from bb_parse import BBParse
from cluster import Cluster


def env_check():
    """
    Checking the running environment
    :return: 
    """
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
    arg_len = len(sys.argv)
    phase_name = ('BENCHMARK', 'LOAD_TEST', 'POWER_TEST', 'THROUGHPUT_TEST_1',
                  'VALIDATE_POWER_TEST', 'VALIDATE_THROUGHPUT_TEST_1')
    if arg_len == 2:
        env_check()
        pat_path = sys.argv[1]
        begin = time.time()
        cluster = Cluster(pat_path)
        cluster.print_cluster_avg()
        stop = time.time()
        print 'elapsed time: {0}'.format(stop - begin)
    elif arg_len == 3:
        env_check()
        pat_path = sys.argv[1]
        bb_log_path = sys.argv[2]
        begin = time.time()
        cluster = Cluster(pat_path)
        bb_parse = BBParse(bb_log_path)
        for phase in phase_name[0:4]:
            start, end = bb_parse.get_stamp_by_phase(phase)
            cluster.print_cluster_avg_by_time(start, end, phase)
        stop = time.time()
        print 'elapsed time: {0}'.format(stop - begin)
    elif arg_len == 4:
        pat_path = sys.argv[1]
        bb_log_path = sys.argv[2]
        phase = sys.argv[3]
        env_check()
        if phase in phase_name:
            begin = time.time()
            cluster = Cluster(pat_path)
            bb_parse = BBParse(bb_log_path)

            start, end = bb_parse.get_stamp_by_phase(phase)
            cluster.print_cluster_avg_by_time(start, end, phase)
            stop = time.time()
            print 'elapsed time: {0}'.format(stop - begin)
        else:
            print 'Supported benchmark phase only includes: {0}'.format(phase_name)
    else:
        print 'Usage: python processing.py $pat_path or python processing.py ' \
              '$pat_path $bb_log_path or python processing.py $pat_path $bb_log_path $BB_Phase\n'
        exit(-1)

        # pat_path = 'C:\\Users\\xuk1\\PycharmProjects\\tmp_data\\pat_cdh511_HoS_27workers_2699v4_72vcores_PCIe_30T_4S_r1'
        # bb_log_path = 'C:\\Users\\xuk1\\PycharmProjects\\tmp_data\\logs_cdh511_HoS_27workers_2699v4_72vcores_PCIe_30T_4S_r1'
