import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
# import pyarrow.dataset as ds
import sys

sys.path.append('/home/xgboost/workspace/xgb/xgboost/python-package')

import xgboost as xgb

INPUT = '/home/xgboost/data/xgboost_4M_float.dataframe.parquet'
#INPUT = '/home/xgboost/data/mortgage_36M.dataframe.parquet'
#INPUT = '/home/xgboost/data/HIGGS-2M.parquet'
#df = pd.read_parquet(INPUT)
#y = df['delinquency_12']
#table = pa.Table.from_pandas(df)
table = pq.read_table(INPUT)
dm4 = xgb.DMatrix(table)
print((dm4.num_row(), dm4.num_col()))
#pt = ds.dataset(INPUT, format='parquet')
#dm2 = xgb.DMatrix(pt)

