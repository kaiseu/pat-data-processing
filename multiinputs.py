#!/usr/bin/python
# encoding: utf-8
"""

@author: xuk1
@license: (C) Copyright 2013-2017
@contact: kai.a.xu@intel.com
@file: multiinputs.py
@time: 10/19/2017 16:20
@desc: 
Processing multi PAT and Log file Pairs, given the parent path, the script will automatically calculate all the pairs.
PAT file starts with 'pat' and exists same postfix log file starts with 'logs', these two files will be regard as a 
pair. PAT file starts with 'PAT' and log file starts with 'LOGS', will also be regarded as a pair.
"""

import os
import subprocess
import sys

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print ('Usage: python multiinputs.py $PATs_PARENT_PATH')
        exit(-1)
    parent_path = sys.argv[1]

    if os.path.exists(parent_path):
        pat_dirs = []
        log_dirs = []
        dirs = next(os.walk(parent_path))[1]  # dir names in $parent_path
        for dir_name in dirs:
            if dir_name.startswith('pat') or dir_name.startswith('PAT'):
                pat_dirs.append(dir_name)
            else:
                print ('No PAT directory starts with "pat" or "PAT" in {0}, exiting...'.format(parent_path))
                exit(-1)
            if dir_name.startswith('logs') or dir_name.startswith('LOGS'):
                log_dirs.append(dir_name)
            else:
                print ('No Log directory starts with "logs" or "LOGS" in {0}, exiting...'.format(parent_path))
                exit(-1)
        assert len(pat_dirs) == len(log_dirs)
        pair = {}
        for pat_dir in pat_dirs:
            for log_dir in log_dirs:
                if pat_dir[3:] == log_dir[4:]:
                    pair[pat_dir] = log_dir
        script_path = os.path.dirname(os.path.abspath(__file__)) + os.sep + 'processing.py'
        if not os.path.exists(script_path):
            print ('Processing script does not exist in path: {0}, will exit...'.format(script_path))
            exit(-1)
        count = 1
        for pat_dir, log_dir in pair.items():
            pat_path = parent_path + os.sep + pat_dir
            log_path = parent_path + os.sep + log_dir
            command = 'python {0} -p {1} -l {2}'.format(script_path, pat_path, log_path)
            print ('Starting to process file pair: {0}, {1}'.format(pat_path, log_path))
            subprocess.call(command, shell=True)
            print ('{0} of total {1} pairs finished.'.format(count, len(pair)))
            count += 1
        print ('All file pairs have been finished!')
    else:
        print ('Path: {0} does not exist, please check and try later.'.format(parent_path))
