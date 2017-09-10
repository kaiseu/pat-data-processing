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

import argparse
import os
import sys
from datetime import datetime
import pandas as pd
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

    try:
        import tables  # noqa
    except ImportError as ex:  # pragma: no cover
        raise ImportError('HDFStore requires PyTables, "{ex}" problem '
                          'importing'.format(ex=str(ex)))

    print '---- You have all required dependencies, starting to process'


def save_avg_result(*option):
    """
    Save results to file
    :param option: optional inputs can be: save_avg_result(pat_path) or save_avg_result(pat_path, bb_log_path) or 
    save_avg_result(pat_path, bb_log_path, BB_Phase) 
    :return: None
    """
    if len(option) == 1:  # only pat_path is assigned
        result_file = option[0] + os.sep + 'results.txt'
        attrib_avg = Cluster(option[0]).get_cluster_data_by_time([0], [0], False)
        with open(result_file, 'w') as f:
            f.write('*' * 110 + '\n')
            f.write('All nodes average utilization\n')
            f.write('*' * 110 + '\n')
            for key in attrib_avg.keys():
                f.write('All nodes average {0} utilization: \n {1} \n'
                        .format(key, attrib_avg.get(key).to_string(index=False)))
                f.write('.' * 75 + '\n')
        print 'Results have been saved to: {0}'.format(result_file)
        return
    elif len(option) == 2:  # pat_path and bb_log are assigned
        result_file = option[0] + os.sep + 'results.txt'
        phase_name = ('BENCHMARK', 'LOAD_TEST', 'POWER_TEST', 'THROUGHPUT_TEST_1',
                      'VALIDATE_POWER_TEST', 'VALIDATE_THROUGHPUT_TEST_1')
        with open(result_file, 'w') as f:
            for phase in phase_name[0:4]:
                start_stamp, end_stamp = BBParse(option[1]).get_stamp_by_phase(phase)
                start_time = datetime.fromtimestamp(start_stamp).strftime('%Y-%m-%d %H:%M:%S')
                end_time = datetime.fromtimestamp(end_stamp).strftime('%Y-%m-%d %H:%M:%S')
                attrib_avg = Cluster(option[0]).get_cluster_avg(start_stamp, end_stamp)
                f.write('*' * 110 + '\n')
                f.write('All nodes average utilization for phase {0} between {1} and {2}:\n'
                        .format(phase, start_time, end_time))
                f.write('*' * 110 + '\n')
                for key in attrib_avg.keys():
                    f.write('All nodes average {0} utilization: \n {1} \n'
                            .format(key, attrib_avg.get(key).to_string(index=False)))
                    f.write('.' * 75 + '\n')
        print 'Results have been saved to: {0}'.format(result_file)
        return
    elif len(option) == 3:  # pat_path, bb_log and phase_name are assigned
        result_file = option[0] + os.sep + 'results.txt'
        with open(result_file, 'w') as f:
            start_stamp, end_stamp = BBParse(option[1]).get_stamp_by_phase(option[2])
            start_time = datetime.fromtimestamp(start_stamp).strftime('%Y-%m-%d %H:%M:%S')
            end_time = datetime.fromtimestamp(end_stamp).strftime('%Y-%m-%d %H:%M:%S')
            attrib_avg = Cluster(option[0]).get_cluster_avg(start_stamp, end_stamp)
            f.write('*' * 110 + '\n')
            f.write('All nodes average utilization for phase {0} between {1} and {2}:\n'
                    .format(option[2], start_time, end_time))
            f.write('*' * 110 + '\n')
            for key in attrib_avg.keys():
                f.write('All nodes average {0} utilization: \n {1} \n'
                        .format(key, attrib_avg.get(key).to_string(index=False)))
                f.write('.' * 75 + '\n')
        print 'Results have been saved to: {0}'.format(result_file)
        return
    else:
        print 'Usage: save_avg_result(pat_path) or save_avg_result(pat_path, bb_log_path) or ' \
              'save_avg_result(pat_path, bb_log_path, BB_Phase)\n'
        exit(-1)


def get_args():
    parse = argparse.ArgumentParser(description='Processing PAT data')
    parse.add_argument('-p', '--pat', type=str, help='PAT file path', required=True)
    parse.add_argument('-l', '--log', type=str, help='TPCx-BB log path', required=False)
    parse.add_argument('-ph', '--phase', type=str, help='TPCx-BB phase', required=False, nargs='+', default='BENCHMARK')
    parse.add_argument('-q', '--query', type=int, help='TPCx-BB query num', required=False, nargs='+')
    parse.add_argument('-s', '--save', type=bool, help='whether to save raw data', required=False, default=False)

    args = parse.parse_args()
    pat_path = args.pat
    log_path = args.log
    phase = args.phase
    query = args.query
    save_raw = args.save
    save_raw = False
    return pat_path, log_path, phase, query, save_raw


def run():
    pat_path, log_path, phase, query, save_raw = get_args()
    if os.path.exists(pat_path):
        if not log_path:  # only pat is assigned
            cluster_avg = Cluster(pat_path).get_cluster_data_by_time([0], [0], save_raw)
            print cluster_avg
        else:
            phase_ts = BBParse(log_path).get_exist_phase_timestamp()
            item_num = {}
            start_stamps = []
            end_stamps = []
            for key, value in phase_ts.items():
                item_num[key] = value.shape[0]
                start_stamps.extend(map(int, (value['epochStartTimestamp']/1000).tolist()))
                end_stamps.extend(map(int, (value['epochEndTimestamp']/1000).tolist()))
            assert len(start_stamps) == len(end_stamps)
            cluster_avg = Cluster(pat_path).get_cluster_data_by_time(start_stamps, end_stamps, save_raw)
            bb_result = pd.concat(phase_ts.values(), axis=0).reset_index(drop=True)
            result = pd.concat(cluster_avg.values(), axis=1)
            # print result
            avg_result = pd.concat([bb_result, result], axis=1)
            result_path = pat_path + os.sep + 'results.txt'
            avg_result.to_csv(result_path, sep=',')
            print avg_result

    else:
        print 'PAT file path: {0} does not exist, exiting...'.format(pat_path)
        exit(-1)


def save_result(cluster_avg, item_num, result_path):
    with open(result_path, 'a') as f:
        for key, value in item_num.items():
            f.write('*' * 100 + '\n')
            f.write('Average {0} utilization: \n {1} \n'.format(key))

        for key, value in cluster_avg.items():
            f.write('*' * 100 + '\n')

            f.write('Average {0} utilization: \n {1} \n'
                    .format(key, value.to_string(index=False)))
            f.write('*' * 100 + '\n')

if __name__ == '__main__':
    print get_args()
    start = time.time()
    run()
    end = time.time()
    print 'Processing elapsed time: {0}'.format(end - start)

#
# if __name__ == '__main__':
#     env_check()
#     if len(sys.argv) == 2:  # only pat_path is assigned
#         tic = time.time()
#         save_avg_result(sys.argv[1])
#         toc = time.time()
#         print 'Processing elapsed time: {0}'.format(toc - tic)
#     elif len(sys.argv) == 3:
#         tic = time.time()
#         save_avg_result(*sys.argv[1:3])
#         toc = time.time()
#         print 'Processing elapsed time: {0}'.format(toc - tic)
#     elif len(sys.argv) == 4:
#         tic = time.time()
#         save_avg_result(*sys.argv[1:4])
#         toc = time.time()
#         print 'Processing elapsed time: {0}'.format(toc - tic)
#     else:
#         print 'Usage: python processing.py $pat_path or python processing.py ' \
#               '$pat_path $bb_log_path or python processing.py $pat_path $bb_log_path $BB_Phase\n'
#         exit(-1)

# pat_path = 'C:\\Users\\xuk1\\PycharmProjects\\tmp_data\\pat_cdh511_HoS_27workers_2699v4_72vcores_PCIe_30T_4S_r1'
# bb_log_path = 'C:\\Users\\xuk1\\PycharmProjects\\tmp_data\\logs_cdh511_HoS_27workers_2699v4_72vcores_PCIe_30T_4S_r1'
