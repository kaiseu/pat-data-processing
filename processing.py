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
import time
from datetime import datetime

import pandas as pd

from bb_parse import BBParse
from cluster import Cluster
from parallelprocessing import get_cluster_data_by_time


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


def str2bool(v):
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')


def get_args():
    """
    Parse command line inputs
    :return: 
    """
    parse = argparse.ArgumentParser(description='Processing PAT data')
    group = parse.add_mutually_exclusive_group()

    parse.add_argument('-p', '--pat', type=str, help='PAT file path', required=True)
    parse.add_argument('-l', '--log', type=str, help='TPCx-BB log path', required=False)
    group.add_argument('-ph', '--phase', type=str, help='TPCx-BB phase', required=False, nargs='+', default='BENCHMARK')
    group.add_argument('-q', '--query', type=int, help='TPCx-BB query num', required=False, nargs='+')
    group.add_argument('-n', '--streamNumber', type=int, help='TPCx-BB stream number', required=False, nargs='+')
    parse.add_argument('-s', '--save', type=str2bool, help='whether to save raw data', required=False, default=False)

    args = parse.parse_args()
    pat_path = args.pat
    log_path = args.log
    phase = args.phase
    stream = args.streamNumber
    query = args.query
    save_raw = args.save
    return pat_path, log_path, phase, stream, query, save_raw


def run():
    env_check()
    pat_path, log_path, phase, stream, query, save_raw = get_args()
    if os.path.exists(pat_path):
        if not log_path:  # only pat_path is assigned
            print 'only pat_path is assigned, calculating BENCHMARK average utilization...\n'
            cluster_avg = Cluster(pat_path).get_cluster_data_by_time([0], [0], save_raw)
            print cluster_avg
        else:  # pat_path and log_path are assigned
            if os.path.exists(log_path):
                phase_ts = BBParse(log_path).get_exist_phase_timestamp()
            else:
                print 'TPCx-BB log file path: {0} does not exist, exiting...'.format(log_path)
                exit(-1)

            start_stamps = []
            end_stamps = []
            if (not query) & (not stream) & (phase == 'BENCHMARK'):  # if -ph -q and -n not assigned
                for key, value in phase_ts.items():
                    start_stamps.extend((value['epochStartTimestamp'] / 1000).tolist())
                    end_stamps.extend((value['epochEndTimestamp'] / 1000).tolist())
                assert len(start_stamps) == len(end_stamps)
                # cluster_avg = Cluster(pat_path).get_cluster_data_by_time(start_stamps, end_stamps, save_raw)
                cluster_avg = get_cluster_data_by_time(pat_path, start_stamps, end_stamps, save_raw)
                bb_result = pd.concat(phase_ts.values(), axis=0).reset_index(drop=True)
                pat_result = pd.concat(cluster_avg.values(), axis=1)
                avg_result = pd.concat([bb_result, pat_result], axis=1)
                result_path = pat_path + os.sep + 'results.txt'
                avg_result.to_csv(result_path, sep=',')
                tag = []
                for key in phase_ts.keys():
                    tag.append(key)
                    if key == 'POWER_TEST':
                        tag.extend(['q' + str(i) for i in phase_ts[key].iloc[1:, 2]])
                    elif key == 'THROUGHPUT_TEST_1':
                        tag.extend(['stream' + str(i) for i in phase_ts[key].iloc[1:, 1]])
                print_result(cluster_avg, tag)
                result_path = pat_path + os.sep + 'pat_avg_all.txt'
                save_result(cluster_avg, tag, result_path)
            elif (not query) & (not stream) & (set(phase).issubset(phase_ts.keys())):  # for BB phase
                for ph in phase:
                    start_stamps.append(int(phase_ts[ph].iloc[0, 3] / 1000))
                    end_stamps.append(int(phase_ts[ph].iloc[0, 4] / 1000))
                assert len(start_stamps) == len(end_stamps)
                # cluster_avg = Cluster(pat_path).get_cluster_data_by_time(start_stamps, end_stamps, save_raw)
                cluster_avg = get_cluster_data_by_time(pat_path, start_stamps, end_stamps, save_raw)
                print_result(cluster_avg, phase)
                result_path = pat_path + os.sep + 'pat_avg.txt'
                save_result(cluster_avg, phase, result_path)
            elif not query:  # for throughput streams
                num_streams = phase_ts['THROUGHPUT_TEST_1'].shape[0] - 1  # num of throughput steams from the log
                if any(s >= num_streams for s in stream):  # check if input streamNumber is right
                    print 'Number of throughput steams is {0}, so input streamNumber should not be ' \
                          'greater than {1}, exiting...'.format(num_streams, num_streams - 1)
                    exit(-1)
                stream = [i + 1 for i in stream]  # index 1 corresponding to stream 0
                start_stamps = map(int, (phase_ts['THROUGHPUT_TEST_1'].iloc[stream, 3] / 1000).tolist())
                end_stamps = map(int, (phase_ts['THROUGHPUT_TEST_1'].iloc[stream, 4] / 1000).tolist())
                assert len(start_stamps) == len(end_stamps)
                # cluster_avg = Cluster(pat_path).get_cluster_data_by_time(start_stamps, end_stamps, save_raw)
                cluster_avg = get_cluster_data_by_time(pat_path, start_stamps, end_stamps, save_raw)
                tag = ['stream' + str(s - 1) for s in stream]  # stream begin from 0
                print_result(cluster_avg, tag)
                result_path = pat_path + os.sep + 'pat_avg.txt'
                save_result(cluster_avg, tag, result_path)
            elif not stream:  # for query
                exist_queries = phase_ts['POWER_TEST'].iloc[:, 2].tolist()
                if not set(query).issubset(set(exist_queries)):  # check if input queries existing in the log
                    print 'Input query may not exist in the log, existing queries are: {0}, ' \
                          'exiting...'.format(exist_queries[1:])
                    exit(-1)
                start_stamps = map(int, (phase_ts['POWER_TEST'].iloc[query, 3] / 1000).tolist())
                end_stamps = map(int, (phase_ts['POWER_TEST'].iloc[query, 4] / 1000).tolist())
                assert len(start_stamps) == len(end_stamps)
                # cluster_avg = Cluster(pat_path).get_cluster_data_by_time(start_stamps, end_stamps, save_raw)
                cluster_avg = get_cluster_data_by_time(pat_path, start_stamps, end_stamps, save_raw)
                tag = ['q' + str(q) for q in query]
                print_result(cluster_avg, tag)
                result_path = pat_path + os.sep + 'pat_avg.txt'
                save_result(cluster_avg, tag, result_path)
            else:
                print 'The input arguments is not supported, exiting...'
                exit(-1)
    else:
        print 'PAT file path: {0} does not exist, exiting...'.format(pat_path)
        exit(-1)


def save_result(cluster_avg, tag, result_path):
    """
    Save result to file
    :param cluster_avg: cluster_avg: dict that contains node avg attribute, e.g. CPU, Disk, Mem, Network
    :param tag: tags for the output index, can be stream number: stream# or query number: q#
    :param result_path: result save path
    :return: 
    """
    with open(result_path, 'w') as f:
        for key, value in cluster_avg.items():
            value = value.set_index([tag])
            f.write('*' * 70 + '\n')
            f.write('Average {0} utilization: \n {1} \n'.format(key, value.to_string()))
        f.write('*' * 70 + '\n')
    print 'Results have been saved to {0}'.format(result_path)


def print_result(cluster_avg, tag):
    """
    print avg result
    :param cluster_avg: dict that contains node avg attribute, e.g. CPU, Disk, Mem, Network
    :param tag: tags for the output index, can be stream number: stream# or query number: q#
    :return: None
    """
    for key, value in cluster_avg.items():
        value = value.set_index([tag])
        print '*' * 70
        print 'Average {0} utilization: \n {1} \n'.format(key, value.to_string()),
    print '*' * 70 + '\n'


if __name__ == '__main__':
    start = time.time()
    run()
    end = time.time()
    print 'Processing elapsed time: {0}'.format(end - start)

    # store = pd.HDFStore('C:\\Users\\xuk1\\PycharmProjects\\tmp_data\\pat_cdh511_HoS_27workers_2699v4_72vcores_PCIe_30T_4S_r1\\instruments\\network.h5').get_storer('network').table
    # print store
# pat_path = 'C:\\Users\\xuk1\\PycharmProjects\\tmp_data\\pat_cdh511_HoS_27workers_2699v4_72vcores_PCIe_30T_4S_r1'
# bb_log_path = 'C:\\Users\\xuk1\\PycharmProjects\\tmp_data\\logs_cdh511_HoS_27workers_2699v4_72vcores_PCIe_30T_4S_r1'
