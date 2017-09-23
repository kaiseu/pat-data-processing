# pat-data-processing
>Processing and visualize PAT data

## Usage: 
```shell
python processing.py -p $pat_path
python processing.py -p $pat_path -l $bb_log_path
python processing.py -p $pat_path -l $bb_log_path -ph $BB_Phase
python processing.py -p $pat_path -l $bb_log_path -n $streamNumber  
python processing.py -p $pat_path -l $bb_log_path -q $query
```


### where:
- $pat_path: is the PAT raw data path
- $bb_log_path: optional, is the corresponding TPCx-BB log path
- $BB_Phase: optional, is the TPCx-BB phase intended to caculate, 
- which includes:'BENCHMARK', 'LOAD_TEST', 'POWER_TEST', 'THROUGHPUT_TEST_1', 'VALIDATE_POWER_TEST', 'VALIDATE_THROUGHPUT_TEST_1'
- $streamNumber: optinal, TPCx-BB throughput stream number, if only wants to calculate certain streams. Can input multi values seperated by space.
- $query: optinal, TPCx-BB query number, if only wants to calculate certain queries. Can input multi values seperated by space.

>The result which contains the average System resource utilization(CPU, Memory, Disk I/O, Network I/O...) of a cluster will be saved in $pat_path/pat_avg_all.txt


## Depedencies: 
- Python 2.7+
- Numpy
- tables 3.3.0

>when collecting data using PAT(Performacne Anaiysis Tool), the worker nodes should be synced with NTP services.


## Known Issue:

1. ImportError: HDFStore requires PyTables, "Could not load any of ['hdf5.dll', 'hdf5dll.dll'], please ensure that it can be found in the system path" problem importing.  
Turns out tables module 3.4.2 has some compatibility problems. Downgrade tables module to 3.3.0 solves this problem.
```shell
 $ pip install tables==3.3.0
```
