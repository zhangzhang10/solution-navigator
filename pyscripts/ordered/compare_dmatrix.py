import numpy as np
import pandas as pd
import pyarrow as pa
# import pyarrow.dataset as ds
import pyarrow.parquet as pq
from pyarrow import csv
import sys

sys.path.append('/home/xgboost/workspace/xgb/xgboost/python-package')

import xgboost as xgb

HIBENCH = '/home/xgboost/data/HiBench10Kx50.dataframe.double.parquet/'
MORTGAGE = '/home/xgboost/data/xgboost_4M_float.dataframe.parquet'
HIGGS2M = '/home/xgboost/data/HIGGS-2M.csv'

df = pd.read_parquet(HIBENCH)
y = df['label']
X = df.drop(columns=['label'])
dm1 = xgb.DMatrix(X, y)

pt = pq.read_table(HIBENCH)
y = pt.column('label').to_numpy()
X = pt.drop(['label'])
dm2 = xgb.DMatrix(X, y)

print('HIBENCH', dm1 == dm2)

arr = np.random.randn(5*1024*1024+1, 251)
arr[arr < 0.5] = np.nan
df = pd.DataFrame(arr)
dm3 = xgb.DMatrix(df)

table = pa.Table.from_pandas(df)
dm4 = xgb.DMatrix(table)

print('SPARSE', dm3 == dm4)

df = pd.read_parquet(MORTGAGE)
y = df['delinquency_12']
X = df.drop(columns=['delinquency_12'])
dm5 = xgb.DMatrix(X, y)

pt = pq.read_table(MORTGAGE)
y = pt.column('delinquency_12').to_numpy()
X = pt.drop(['delinquency_12'])
dm6 = xgb.DMatrix(X, y)

print('MORTGAGE', dm5 == dm6)

df = pd.read_csv(HIGGS2M)
y = df['f0']
X = df.drop(columns=['f0'])
dm9 = xgb.DMatrix(X, y)

pt = csv.read_csv(HIGGS2M)
y = pt.column('f0')
X = pt.drop(['f0'])
dm10 = xgb.DMatrix(X, y)

print('HIGGS2M', dm9 == dm10)

