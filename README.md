# pat-data-processing
processing and visualize PAT data
Usage: python processing.py $pat_path or python processing.py $pat_path $bb_log_path

or python processing.py $pat_path $bb_log_path $BB_Phase

$pat_path is the PAT raw data path

$bb_log_path is the corresponding TPCx-BB log path

$BB_Phase is the TPCx-BB phase intended to caculate, 
which includes:'BENCHMARK', 'LOAD_TEST', 'POWER_TEST', 'THROUGHPUT_TEST_1', 'VALIDATE_POWER_TEST', 'VALIDATE_THROUGHPUT_TEST_1'


