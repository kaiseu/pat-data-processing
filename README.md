# pat-data-processing
Processing and visualize PAT data

Usage: python processing.py $pat_path or python processing.py $pat_path $bb_log_path

or python processing.py $pat_path $bb_log_path $BB_Phase

$pat_path: is the PAT raw data path

$bb_log_path: optional, is the corresponding TPCx-BB log path

$BB_Phase: optional, is the TPCx-BB phase intended to caculate, 
which includes:'BENCHMARK', 'LOAD_TEST', 'POWER_TEST', 'THROUGHPUT_TEST_1', 'VALIDATE_POWER_TEST', 'VALIDATE_THROUGHPUT_TEST_1'

The result which contains the average System resource utilization(CPU, Memory, Disk I/O, Network I/O...) of a cluster will be saved in $pat_path/instruments/results.txt


Depedencies: 
Python 2.7+
Numpy
tables 3.3.0




Known Issue:

ImportError: HDFStore requires PyTables, "Could not load any of ['hdf5.dll', 'hdf5dll.dll'], please ensure that it can be found in the system path" problem importing

Turns out tables module 3.4.2 has some compatibility problems. Downgrade tables module to 3.3.0 solves this problem.
 $ pip install tables==3.3.0
