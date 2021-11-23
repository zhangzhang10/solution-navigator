import sys
import timeit

sys.path.append('/home/xgboost/workspace/xgb/xgboost/python-package')

NREPEAT = 1
NUMBER = 1

INPUT = '/home/xgboost/data/mortgage_3.5G_first100.csv'

setup = '''
import pandas as pd
import xgboost as xgb
df = pd.read_csv({!r}, header=None, usecols=list(range(0,46)))
'''.format(INPUT)
test = '''
dmat = xgb.DMatrix(df)
'''

print('With Pandas: {} seconds'.format(min(
    timeit.repeat(setup = setup, stmt = test,
                  repeat = NREPEAT, number = NUMBER))/NUMBER))

setup = '''
import pandas as pd
import pyarrow as pa
import xgboost as xgb
df = pd.read_csv({!r}, header=None, usecols=list(range(0,46)))
table = pa.Table.from_pandas(df)
'''.format(INPUT)
test = '''
dmat = xgb.DMatrix(table)
'''

print('With Arrow: {} seconds'.format(min(
    timeit.repeat(setup = setup, stmt = test,
                  repeat = NREPEAT, number = NUMBER))/NUMBER))

