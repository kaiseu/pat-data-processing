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
import re
from collections import OrderedDict

import numpy as np
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
        Parse TPCx-BB each phase's timestamps from TPCx-BB log file BigBenchTimes.csv. 
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
                    phase_ts[phase].iloc[0, 2] = 0  # fill 0 to blank value
                elif phase == 'THROUGHPUT_TEST_1':  # throughput test overall and each stream
                    query_num = (pd.isnull(df['queryNumber']))
                    mask = benchmark_phase & query_num
                    phase_ts[phase] = df[mask].reset_index(drop=True)
                    phase_ts[phase].iloc[0, 1:3] = -1  # file -1 to overall throughput test
                    phase_ts[phase].iloc[1:, 2] = 0  # fill 0 to blank value
                else:  # other phases
                    stream_num = (pd.isnull(df['streamNumber']))
                    query_num = (pd.isnull(df['queryNumber']))
                    mask = benchmark_phase & stream_num & query_num
                    # phase_start = line['epochStartTimestamp'].values / 1000
                    # phase_end = line['epochEndTimestamp'].values / 1000
                    phase_ts[phase] = df[mask].reset_index(drop=True)
                    phase_ts[phase].iloc[0, 1:3] = 0  # fill 0 to blank value
                phase_ts[phase] = phase_ts[phase].astype(converter)
                is_exist = True
        if is_exist:
            return phase_ts
        else:
            print 'It seems BigBenchTimes.csv in {0} does not include any TPCx-BB phases, ' \
                  'existing...'.format(self.bb_log_path)
            exit(-1)

    def get_elapsed_time(self):
        """
        Get TPCx-BB elapsed time of each query in all the phases from BigBenchTimes.csv file
        :return: Results will be saved in $bb_log_path/bb_results.log
        """
        self.get_bb_result()
        csv_path = self.bb_log_path + os.sep + 'run-logs' + os.sep + 'BigBenchTimes.csv'
        if not os.path.isfile(csv_path):
            print 'BigBenchTimes.csv does not exist in {0}, existing...'.format(self.bb_log_path)
            exit(-1)
        df = pd.read_csv(csv_path, delimiter=';').loc[:,
             ['benchmarkPhase', 'streamNumber', 'queryNumber', 'durationInSeconds']]
        elapsed_time = pd.DataFrame()
        is_exist = False
        for phase in ['POWER_TEST', 'THROUGHPUT_TEST_1']:
            benchmark_phase = (df['benchmarkPhase'] == phase)
            if any(benchmark_phase):  # whether this phase exist in the BB logs
                if phase == 'POWER_TEST':  # power test overall and each query
                    stream_num = ((df['streamNumber']) == 0)
                    query_num = (pd.notnull(df['queryNumber']))
                    mask = benchmark_phase & stream_num & query_num
                    seconds = df[mask]['durationInSeconds'].values
                    elapsed_time.insert(0, phase, seconds)
                    elapsed_time.index = df[mask]['queryNumber'].astype('int64')
                elif phase == 'THROUGHPUT_TEST_1':
                    streams = int(np.max(df['streamNumber']))
                    for stream in range(streams + 1):
                        stream_num = ((df['streamNumber']) == stream)
                        query_num = (pd.notnull(df['queryNumber']))
                        mask = benchmark_phase & stream_num & query_num
                        seconds = df[mask]['durationInSeconds'].values
                        elapsed_time.insert(stream + 1, 'stream{0}'.format(stream), seconds)
                        elapsed_time.index = df[mask]['queryNumber'].astype('int64')
                is_exist = True
        if is_exist:
            print '*' * 100
            print 'Elapsed time of each query:\n {0} \n'.format(elapsed_time.to_string())

            result_path = self.bb_log_path + os.sep + 'bb_results.log'
            with open(result_path, 'a') as f:
                f.write('*' * 100 + '\n')
                f.write('Elapsed time of each query:\n {0} \n'.format(elapsed_time.to_string()))
        else:
            print 'It seems BigBenchTimes.csv in {0} does not include TPCx-BB phases:POWER_TEST, THROUGHPUT_TEST_1' \
                  'existing...'.format(self.bb_log_path)
            exit(-1)

    def get_bb_result(self):
        """
        Parse TPCx-BB score from BigBenchResult.log file
        :return: Results will be saved in $bb_log_path/bb_results.log
        """
        log_path = self.bb_log_path + os.sep + 'run-logs' + os.sep + 'BigBenchResult.log'
        if not os.path.isfile(log_path):
            print 'BigBenchResult.log does not exist in {0}, existing...'.format(self.bb_log_path)
            exit(-1)
        result = OrderedDict()
        with open(log_path, 'r') as f:
            for line in f:
                if re.match('(.*)T_LOAD = (.*)', line):
                    T_LOAD = line.split('=')[1].strip()
                    result['T_LOAD'] = float(T_LOAD)
                if re.match('(.*)T_LD = (.*)', line):
                    T_LD = line.split(':')[2].strip()
                    result['T_LD'] = float(T_LD)
                if re.match('(.*)T_PT = (.*)', line):
                    T_PT = line.split('=')[1].strip()
                    result['T_PT'] = float(T_PT)
                if re.match('(.*)T_T_PUT = (.*)', line):
                    T_T_PUT = line.split('=')[1].strip()
                    result['T_T_PUT'] = float(T_T_PUT)
                if re.match('(.*)T_TT = (.*)', line):
                    T_TT = line.split('=')[1].strip()
                    result['T_TT'] = float(T_TT)
                if re.match('(.*)VALID BBQpm@(.*)', line):
                    key = line.split('=')[0].split(' ')[2].strip()
                    value = line.split('=')[1].strip()
                    result[key] = float(value)
        result_path = self.bb_log_path + os.sep + 'bb_results.log'
        print ('*' * 100)
        print ('TPCx-BB results:')
        with open(result_path, 'w') as f:
            f.write('*' * 100 + '\n')
            f.write('TPCx-BB results: \n')
            for key, value in result.items():
                print '{0}: {1}'.format(key, value)
                f.write(key + ': ' + str(value) + '\n')
            f.write('\n')
