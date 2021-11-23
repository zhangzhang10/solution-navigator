from concurrent.futures import ThreadPoolExecutor
import resource
import sys
from time import sleep

import pyarrow.parquet as pq

sys.path.append('/home/xgboost/workspace/xgb/xgboost/python-package')
import xgboost as xgb


class MemoryMonitor:
    def __init__(self):
        self.keep_measuring = True

    def measure_usage(self):
        max_usage = 0
        while self.keep_measuring:
            max_usage = max(
                max_usage,
                resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            )
            sleep(0.1)

        return max_usage

INPUT = '/home/xgboost/data/mortgage_150M.dataframe.parquet'

def dmat_with_table(dfile):
    table = pq.read_table(dfile)
    dm = xgb.DMatrix(table)
    return dm

if __name__ == "__main__":
    with ThreadPoolExecutor() as executor:
        monitor = MemoryMonitor()
        mem_thread = executor.submit(monitor.measure_usage)
        try:
            fn_thread = executor.submit(dmat_with_table, INPUT)
            result = fn_thread.result()
        finally:
            monitor.keep_measuring = False
            max_usage = mem_thread.result()
            
        print("Peak memory usage when using pyarrow.Table:",
                f"{max_usage/1_000_000} GiB")

