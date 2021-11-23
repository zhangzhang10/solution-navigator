import sys
import timeit

sys.path.append('/home/xgboost/workspace/xgb/xgboost/python-package')

NREPEAT = 3
NUMBER = 3
#INPUT = '/home/xgboost/data/xgboost_4M_float.dataframe.parquet/part-00000-ab00ad58-a1cc-4a24-83e7-7984d80b800c-c000.snappy.parquet'
INPUT = '/home/xgboost/data/xgboost_4M_float.dataframe.parquet'
#INPUT = '/home/xgboost/data/HiBench10Kx50.dataframe.double.parquet/'
#INPUT = '/home/xgboost/data/HIGGS-2M.csv'
#INPUT = '/home/xgboost/data/mortgage_36M.dataframe.parquet'

setup = '''
import pandas as pd
import xgboost as xgb
'''
test = '''
df = pd.read_parquet({!r})
y = df['delinquency_12']
X = df[df.columns.difference(['delinquency_12'])]
dm = xgb.DMatrix(X, y)
'''.format(INPUT)

print('With Pandas: {:.2f} seconds'.format(min(
    timeit.repeat(
        setup=setup, stmt=test, repeat=NREPEAT, number=NUMBER))/NUMBER))

# setup = '''
# import pyarrow.dataset as ds
# import xgboost as xgb
# '''
# test = '''
# dataset = ds.dataset({!r}, format='parquet')
# dm = xgb.DMatrix(dataset, 'delinquency_12')
# '''.format(INPUT)
# 
# print('With Arrow dataset: {:.2f} seconds'.format(min(
#     timeit.repeat(
#         setup=setup, stmt=test, repeat=NREPEAT, number=NUMBER))/NUMBER))
 
setup = '''
import pyarrow.parquet as pq
import xgboost as xgb
'''
test = '''
table = pq.read_table({!r})
dm = xgb.DMatrix(table, label='delinquency_12')
'''.format(INPUT)

print('With Arrow table: {:.2f} seconds'.format(min(
    timeit.repeat(
        setup=setup, stmt=test, repeat=NREPEAT, number=NUMBER))/NUMBER))

#setup = '''
#import pandas as pd
#import xgboost as xgb
#'''
#test = '''
#df = pd.read_csv({!r})
#y = df['delinquency_12']
#X = df[df.columns.difference(['delinquency_12'])]
#dm = xgb.DMatrix(X, y)
#'''.format(INPUT)
#
#print('With Pandas: {:.2f} seconds'.format(min(
#    timeit.repeat(
#        setup=setup, stmt=test, repeat=NREPEAT, number=NUMBER))/NUMBER))
#
#setup = '''
#from pyarrow import csv
#import xgboost as xgb
#'''
#test = '''
#table = csv.read_csv({!r})
#dm = xgb.DMatrix(table, label='delinquency_12')
#'''.format(INPUT)
#
#print('With Arrow table: {:.2f} seconds'.format(min(
#    timeit.repeat(
#        setup=setup, stmt=test, repeat=NREPEAT, number=NUMBER))/NUMBER))
