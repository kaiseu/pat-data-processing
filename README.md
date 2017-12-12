# pat-data-processing
>Processing and visualize PAT data

## Usage: 
>Processing PAT and TPCx-BB log data:
```shell
python processing.py -p $pat_path
python processing.py -p $pat_path -l $bb_log_path
python processing.py -p $pat_path -l $bb_log_path -ph $BB_Phase
python processing.py -p $pat_path -l $bb_log_path -n $streamNumber  
python processing.py -p $pat_path -l $bb_log_path -q $query
python processing.py -p $pat_path -l $bb_log_path -s true
```

### where:
- $pat_path: is the PAT raw data path
- $bb_log_path: optional, is the corresponding TPCx-BB log path
- $BB_Phase: optional, is the TPCx-BB phase intended to caculate, 
- which includes:'BENCHMARK', 'LOAD_TEST', 'POWER_TEST', 'THROUGHPUT_TEST_1', 'VALIDATE_POWER_TEST', 'VALIDATE_THROUGHPUT_TEST_1'
- $streamNumber: optinal, TPCx-BB throughput stream number, if only wants to calculate certain streams. Can input multi values seperated by space.
- $query: optinal, TPCx-BB query number, if only wants to calculate certain queries. Can input multi values seperated by space.
- -s, whether to save raw result data, default is false.


>Processing multi PAT and TPCx-BB log pairs:
```shell
python multiinputs.py $parent_path
```
### where:
- $parent_path: is the parent dir path of the PAT file pairs.
PAT file starts with 'pat' and exists same postfix log file starts with 'logs', these two files will be regard as a
pair. PAT file starts with 'PAT' and log file starts with 'LOGS', will also be regarded as a pair.


>Processing only the TPCx-BB log file:
```shell
 python bb_parse.py $BB_Log_path
```
### where
- $BB_Log_path: is the path of TPCx-BB log files.


>The PAT result which contains the average System resource utilization(CPU, Memory, Disk I/O, Network I/O...) of a cluster will be saved in $pat_path/pat_avg_all.txt.

>The TPCx-BB log of elapsed time will be saved in $BB_Log_path/bb_results.log.


## Depedencies: 
- Python 2.7+(on the python2 series), Python 3.x+ is not tested yet
- Numpy
- tables 3.3.0

>when collecting data using [PAT](https://github.com/intel-hadoop/PAT)(Performacne Anaiysis Tool), the worker nodes should be synced with NTP services.


## Known Issue:

1. ImportError: HDFStore requires PyTables, "Could not load any of ['hdf5.dll', 'hdf5dll.dll'], please ensure that it can be found in the system path" problem importing.  
Turns out tables module 3.4.2 has some compatibility problems. Downgrade tables module to 3.3.0 solves this problem.
```shell
 $ pip install tables==3.3.0
```
