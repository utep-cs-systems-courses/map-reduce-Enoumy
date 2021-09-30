# Parallel Computing Assignment 2 - Jose Rodriguez

> Map Reduce

The report for this assignment is contained inside of `report.pdf`. I have
also included different bash scripts to run the tests that I have included in
my report. The code for this assignmnet is contained inside of `solution.py`.

```sh
sh run.sh
sh run_with_granular_timing.sh
sh run_with_output.sh
```

## Instructions on how to run

In order to get full documentation on the instructions and flag configurability
for the assignment run the following:

```sh
python3 solution.py --help
```

Here is full focumentation of the flags that this program supports:

```
usage: solution.py [-h] [--silent SILENT] [--num_threads NUM_THREADS] [--no_pymp NO_PYMP]
                   [--time_file_reading TIME_FILE_READING] [--time_counting TIME_COUNTING]

Map Reduce assignment. Counts words inside of Shakespeare's works!

optional arguments:
  -h, --help            show this help message and exit
  --silent SILENT       If set to true, will only print timing results in addition to the number of
                        threads
  --num_threads NUM_THREADS
                        Number of threads which will be created in order to count the number of
                        words.
  --no_pymp NO_PYMP     If set to true, a synchronous version that does not use pymp will run.
  --time_file_reading TIME_FILE_READING
                        If set to true, this will time the file reading operation
  --time_counting TIME_COUNTING
                        If set to true, this will time the counting operations
```

There are examples on how to use the program inside of the previously mentioned
testing scripts `run.sh`, `run_with_granular_timing.sh`, and `run_with_output`.
