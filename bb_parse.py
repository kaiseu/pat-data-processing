#!/usr/bin/python
# encoding: utf-8
"""

@author: xuk1
@license: (C) Copyright 2013-2017
@contact: kai.a.xu@intel.com
@file: bb_parse.py
@time: 8/28/2017 11:03
@desc: 

"""

import os
from collections import OrderedDict

import pandas as pd


class BBParse:
    """
    Parse TPCx-BB log from BigBenchTimes.csv
    """
    phase_name = ['BENCHMARK', 'LOAD_TEST', 'POWER_TEST', 'THROUGHPUT_TEST_1',
                  'VALIDATE_POWER_TEST', 'VALIDATE_THROUGHPUT_TEST_1']

    def __init__(self, bb_log_path):
        """
        Constructor for parse TPCx-BB log
        :param bb_log_path: TPCx-BB log path, dir 'run-logs' is not included
        """
        self.bb_log_path = bb_log_path
        pass

    def get_stamp_by_phase(self, phase, *stream_query_num):
        """
        get start and end timestamp of each phase or each query from TPCx-BB logs
        :param phase: phase names includes: ['BENCHMARK', 'LOAD_TEST', 'POWER_TEST', 
        'THROUGHPUT_TEST_1', 'VALIDATE_POWER_TEST', 'VALIDATE_THROUGHPUT_TEST_1']
        :param stream_query_num: optional, maximum two inputs are allowed, 
        one is for the stream number the other is for the query number
        :return: start and end timestamp
        """
        if phase in self.phase_name:
            csv_path = self.bb_log_path + os.sep + 'run-logs' + os.sep + 'BigBenchTimes.csv'
            if not os.path.isfile(csv_path):
                print 'BigBenchTimes.csv does not exist in {0}, existing...'.format(self.bb_log_path)
                exit(-1)
            df = pd.read_csv(csv_path, delimiter=';').loc[:,
                 ['benchmarkPhase', 'streamNumber', 'queryNumber', 'epochStartTimestamp', 'epochEndTimestamp']]
            benchmark_phase = (df['benchmarkPhase'] == phase)
            if not stream_query_num:  # if stream_query_num is none, this mean only phase
                stream_num = (pd.isnull(df['streamNumber']))
                query_num = (pd.isnull(df['queryNumber']))
                mask = benchmark_phase & stream_num & query_num
                line = df[mask]
                phase_start = line['epochStartTimestamp'].values / 1000
                phase_end = line['epochEndTimestamp'].values / 1000
                return phase_start, phase_end
            elif len(stream_query_num) == 1:  # stream num is assigned
                if phase == 'BENCHMARK':
                    print 'BENCHMARK does not have stream number, you can only use ("BENCHMARK")'
                if (phase == 'POWER_TEST') & (int(stream_query_num[0]) > 0):
                    print 'POWER_TEST does not have stream number greater than 0,' \
                          ' you can use either ("POWER_TEST") or ("POWER_TEST", 0))'
                    exit(-1)
                stream_num = ((df['streamNumber']) == int(stream_query_num[0]))
                query_num = (pd.isnull(df['queryNumber']))
                mask = benchmark_phase & stream_num & query_num
                line = df[mask]
                phase_start = line['epochStartTimestamp'].values / 1000
                phase_end = line['epochEndTimestamp'].values / 1000
                return phase_start, phase_end
            elif len(stream_query_num) == 2:  # query num is assigned
                if phase == 'BENCHMARK':
                    print 'BENCHMARK does not have stream number or query number, you can only use ("BENCHMARK")'
                if (phase == 'POWER_TEST') & (int(stream_query_num[0]) > 0):
                    print 'POWER_TEST does not have stream number greater than 0,' \
                          ' you can use either ("POWER_TEST", "0", "query_num"))'
                    exit(-1)
                stream_num = ((df['streamNumber']) == int(stream_query_num[0]))
                query_num = ((df['queryNumber']) == int(stream_query_num[1]))
                mask = benchmark_phase & stream_num & query_num
                line = df[mask]
                phase_start = line['epochStartTimestamp'].values / 1000
                phase_end = line['epochEndTimestamp'].values / 1000
                return phase_start, phase_end
            else:
                print 'maximum 3 inputs are allowed'
                exit(-1)
        else:
            print 'phase name must be assigned! phase name only includes: {0}'.format(self.phase_name)
            exit(-1)

    def get_exist_phase_timestamp(self):
        """
        Parse TPCx-BB  each phase's timestamps from TPCx-BB log file BigBenchTimes.csv. 
        For POWER_TEST each query will included, for THROUGHPUT_TEST_1, each stream will included
        :return: dict that contains start and end timestamps of each phase
        """
        csv_path = self.bb_log_path + os.sep + 'run-logs' + os.sep + 'BigBenchTimes.csv'
        if not os.path.isfile(csv_path):
            print 'BigBenchTimes.csv does not exist in {0}, existing...'.format(self.bb_log_path)
            exit(-1)
        converter = {'benchmarkPhase': str, 'streamNumber': int, 'queryNumber': int,
                     'epochStartTimestamp': 'int64', 'epochEndTimestamp': 'int64'}
        df = pd.read_csv(csv_path, delimiter=';').loc[:,
             ['benchmarkPhase', 'streamNumber', 'queryNumber', 'epochStartTimestamp', 'epochEndTimestamp']]
        phase_ts = OrderedDict()
        is_exist = False
        for phase in self.phase_name:
            benchmark_phase = (df['benchmarkPhase'] == phase)
            if any(benchmark_phase):  # whether this phase exist in the BB logs
                if phase == 'POWER_TEST':  # power test overall and each query
                    stream_num = ((df['streamNumber']) == 0)
                    mask = benchmark_phase & stream_num
                    phase_ts[phase] = df[mask].reset_index(drop=True)
                    phase_ts[phase].iloc[0, 2] = 0  # file 0 to blank value
                elif phase == 'THROUGHPUT_TEST_1':  # throughput test overall and each stream
                    query_num = (pd.isnull(df['queryNumber']))
                    mask = benchmark_phase & query_num
                    phase_ts[phase] = df[mask].reset_index(drop=True)
                    phase_ts[phase].iloc[0, 1:3] = -1  # file -1 to overall throughput test
                    phase_ts[phase].iloc[1:, 2] = 0  # file 0 to blank value
                else:  # other phases
                    stream_num = (pd.isnull(df['streamNumber']))
                    query_num = (pd.isnull(df['queryNumber']))
                    mask = benchmark_phase & stream_num & query_num
                    # phase_start = line['epochStartTimestamp'].values / 1000
                    # phase_end = line['epochEndTimestamp'].values / 1000
                    phase_ts[phase] = df[mask].reset_index(drop=True)
                    phase_ts[phase].iloc[0, 1:3] = 0  # file 0 to blank value
                phase_ts[phase] = phase_ts[phase].astype(converter)
                is_exist = True
        if is_exist:
            return phase_ts
        else:
            print 'It seems BigBenchTimes.csv in {0} does not include any TPCx-BB phases, ' \
                  'existing...'.format(self.bb_log_path)
            exit(-1)
