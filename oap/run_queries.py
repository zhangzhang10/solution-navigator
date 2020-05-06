#!/usr/bin/python3

import os
import sys
import time
import json

#spark_home = '/home/spark-sql/spark-2.3.2.vmemcache'

beeline_cmd = spark_home + '/bin/beeline'
beeline_args = '-u jdbc:hive2://hostname-master:10001 -n * -p *'
database_name = 'tpcds2048'
query_dir = './tpcds-queries-fifo/'

log_dir = './log/'
log_file_suffix = 'tpcds2048-aep-'
log_file = log_dir + log_file_suffix + 'result.json'

queries = [26, 27, 68, 42, 53, 32, 76, 48, 55]
times = 3 
result_dict = {}

def copy_to_tmp_file(query):
    path_pre = query.split('.')[0]
    tmp_file = path_pre + '_tmp.sql'
    with open(query, 'r') as rf:
        with open(tmp_file, 'w') as wf:
            wf.write('use {0};\n'.format(database_name))
            for line in rf.readlines():
                wf.write(line)
            wf.write(';')
    return tmp_file

for i in range(times):

    for query in queries:
        query_path = (query_dir + 'q{0}.sql').format(query)
        tmp_query_path = copy_to_tmp_file(query_path)
        start = time.time()
        cmd = beeline_cmd + ' ' \
            + beeline_args + ' '\
            + '-f {0}'
        os.system(cmd.format(tmp_query_path))

        duration = round((time.time() - start), 2)

        if query in result_dict:
            result_dict[query].append((i, duration))
        else:
            result_dict[query] = [(i, duration)]

    if os.path.exists(tmp_query_path):
        os.remove(tmp_query_path)

with open(log_file, 'w') as f:
    f.write(json.dumps(result_dict))

