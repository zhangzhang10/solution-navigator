import numpy as np
import pandas as pd
import pyarrow as pa
import sys

sys.path.append('/home/xgboost/workspace/xgb/xgboost/python-package')

import xgboost as xgb

arr = np.random.rand(1000001, 157)
arr[arr < 0.5] = np.nan

df = pd.DataFrame(arr)
dm1 = xgb.DMatrix(df)

table = pa.Table.from_pandas(df)
dm2 = xgb.DMatrix(table)

print(dm2 == dm1)
