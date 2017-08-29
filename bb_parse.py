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
import pandas as pd


class BBPhase:
    """
    Phase TPCx-BB log from BigBenchTimes.csv
    """
    phase_name = ['BENCHMARK', 'LOAD_TEST', 'POWER_TEST', 'THROUGHPUT_TEST_1', 'VALIDATE_POWER_TEST', 'VALIDATE_THROUGHPUT_TEST_1']

    def __init__(self, bb_log_path):
        """
        Constructor for phase TPCx-BB log
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
                print line
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
                print line
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
                print line
                phase_start = line['epochStartTimestamp'].values / 1000
                phase_end = line['epochEndTimestamp'].values / 1000
                return phase_start, phase_end
            else:
                print 'maximum 3 inputs are allowed'
                exit(-1)
        else:
            print 'phase name must be assigned! phase name only includes: {0}'.format(self.phase_name)
            exit(-1)


if __name__ == '__main__':
    bb_phase = BBPhase('C:\\Users\\xuk1\\PycharmProjects\\tmp_data\\logs_spark163_1TB_r1')
    bb_phase.get_stamp_by_phase('POWER_TEST')
