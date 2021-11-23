import sys
import timeit

sys.path.append('/home/xgboost/workspace/xgb/xgboost/python-package')

NREPEAT = 3
NUMBER = 3

#setup = '''
#import numpy as np
#import pandas as pd
#import xgboost as xgb
#arr = np.random.rand(5_000_000, 50)
#arr[arr < 0.2] = np.nan
#df = pd.DataFrame(arr)
#'''
#test = '''
#dmat = xgb.DMatrix(df)
#'''
#
#print('With Pandas: {} seconds'.format(min(
#    timeit.repeat(setup = setup, stmt = test,
#                  repeat = NREPEAT, number = NUMBER))/NUMBER))

setup = '''
import numpy as np
import pandas as pd
import pyarrow as pa
import xgboost as xgb
arr = np.random.rand(5_000_000, 50)
arr[arr < 0.2] = np.nan
df = pd.DataFrame(arr)
table = pa.Table.from_pandas(df)
'''
test = '''
dmat = xgb.DMatrix(table)
'''

print('With Arrow: {} seconds'.format(min(
    timeit.repeat(setup = setup, stmt = test,
                  repeat = NREPEAT, number = NUMBER))/NUMBER))

