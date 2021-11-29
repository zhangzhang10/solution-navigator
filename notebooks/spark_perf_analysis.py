import pyspark.sql.functions as F
import json
import builtins
from pyspark.sql.types import (StructType, StructField, DateType,
    TimestampType, StringType, LongType, IntegerType, DoubleType,FloatType)
from pyspark.sql.functions import pandas_udf, PandasUDFType
import math
from functools import reduce
import re
import collections
from pyspark.ml import Pipeline
import pandas
import numpy
import xml.etree.ElementTree as ET

def splits(x):
    fi=[]
    for l in x:
        li=re.split(r'\s+',l)
        for j in range(len(li),118):
            li.append('')
        fi.append(li)
    return iter(fi)

def count_sum(collected_cores):
    return F.expr('+'.join(['_{:d}/_2*{:d}'.format(c+3,tsc_freq) for c in collected_cores]))

def cores_sum(collected_cores):
    return count_sum(collected_cores)

def mem_sum(collected_cores):
    return count_sum(list(range(12)))

def get_alias_name(metric,func):
    return metric+"_"+func.__name__

class App_Log_Analysis:
    def __init__(self, appid,jobids, spark):
        self.appid=appid
        self.jobids=jobids
        self.starttime=0
        self.selectjob=None
        self.spark = spark
        
    def load_log(self):
        jobids=self.jobids
        df=self.spark.read.json("/tmp/sparkEventLog/"+self.appid+"*")
        jobstart=df.where("event='SparkListenerJobStart'").select('job id','Submission Time')
        jobend=df.where("event='SparkListenerJobEnd'").select('job id','Completion Time')
        jobs=jobstart.join(jobend,'job id')
        task=df.where("(Event='SparkListenerTaskEnd' or Event='SparkListenerTaskStart') ").select("Event","Stage ID","task info.*","task metrics.*")
        taskjob=task.join(jobs,[task['Launch Time']>=jobs["Submission Time"],task["Launch Time"]<=jobs["Completion Time"]]).\
            select("Host","`Event`","`Launch Time`","`Executor ID`","`Task ID`","`Finish Time`",
                    "`Stage ID`","`job id`","`Input Metrics`.`Bytes Read`","`Memory Bytes Spilled`","`Shuffle Read Metrics`.`Local Bytes Read`","`Shuffle Read Metrics`.`Remote Bytes Read`",
                   "`Shuffle Write Metrics`.`Shuffle Bytes Written`","`Executor Deserialize Time`","`Shuffle Read Metrics`.`Fetch Wait Time`","`Executor Run Time`","`Shuffle Write Metrics`.`Shuffle Write Time`",
                   "`Result Serialization Time`","`Getting Result Time`","`JVM GC Time`","`Executor CPU Time`",
                    F.when(task['Finish Time']==0,task['Launch Time']).otherwise(task['Finish Time']).alias('eventtime')
        )
        self.selectjob=taskjob.where('`job id` in ({:s})'.format(','.join(jobids))).orderBy(["eventtime", "Finish Time"], ascending=[1, 0])
        self.selectjob.cache()
        
    def generate_log_trace_view(self,showcpu=False):
        
        if self.selectjob is None:
            self.load_log()
        
        appid=self.appid
        events=self.selectjob.toPandas()
        coretrack={}
        trace_events=[]
        starttime=0
        taskend=[]
        trace={"traceEvents":[]}
        exec_hosts={}
        hostsdf=self.selectjob.select("Host").distinct().orderBy("Host")
        hostid=100000
        ended_event=[]

        for i,l in hostsdf.toPandas().iterrows():
            exec_hosts[l['Host']]=hostid
            hostid=hostid+100000

        for idx,l in events.iterrows():
            if l['Event']=='SparkListenerTaskStart':
                hostid=exec_hosts[l['Host']]

                tsk=l['Task ID']
                pid=int(l['Executor ID'])*100+hostid
                stime=l['Launch Time']
                #the task's starttime and finishtime is the same, ignore it.
                if tsk in ended_event:
                    continue
                if not pid in coretrack:
                    tids={}
                    trace_events.append({
                       "name": "process_name",
                       "ph": "M",
                       "pid":pid,
                       "tid":0,
                       "args":{"name":"{:s}.{:s}".format(l['Host'],l['Executor ID'])}
                      })

                else:
                    tids=coretrack[pid]
                for t in tids.keys():
                    if tids[t][0]==-1:
                        tids[t]=[tsk,stime]
                        break
                else:
                    t=len(tids)
                    tids[t]=[tsk,stime]
                #print("task {:d} tid is {:s}.{:d}".format(tsk,pid,t))
                coretrack[pid]=tids

            if l['Event']=='SparkListenerTaskEnd':
                sevt={}
                eevt={}
                hostid=exec_hosts[l['Host']]
                pid=int(l['Executor ID'])*100+hostid
                tsk=l['Task ID']
                fintime=l['Finish Time']

                tids=coretrack[pid]
                for t in tids.keys():
                    if tids[t][0]==tsk:
                        tids[t]=[-1,-1]
                        break
                else:
                    ended_event.append(tsk)
                    continue
                for ps in reversed([key for key in tids.keys()]) :
                    if tids[ps][1]-fintime<0 and tids[ps][1]-fintime>=-2:
                        fintime=tids[ps][1]
                        tids[t]=tids[ps]
                        tids[ps]=[-1,-1]
                        break
                if starttime==0:
                    starttime=l['Launch Time']

                sstime=l['Launch Time']-starttime

                trace_events.append({
                       'tid':pid+int(t),
                       'ts':sstime,
                       'dur':fintime-l['Launch Time'],
                       'pid':pid,
                       "ph":'X',
                       'name':"stg{:d}".format(l['Stage ID']),
                       'args':{"job id": l['job id'],
                               "stage id": l['Stage ID'],
                               "tskid":tsk,
                               "input":builtins.round(l["Bytes Read"]/1024/1024,2),
                               "spill":builtins.round(l["Memory Bytes Spilled"]/1024/1024,2),
                               "Shuffle Read Metrics": "",
                               "|---Local Read": builtins.round(l["Local Bytes Read"]/1024/1024,2),
                               "|---Remote Read":builtins.round(l["Remote Bytes Read"]/1024/1024,2),
                               "Shuffle Write Metrics": "",
                               "|---Write":builtins.round(l['Shuffle Bytes Written']/1024/1024,2)
                               }
                      })

                des_time=l['Executor Deserialize Time']
                read_time=l['Fetch Wait Time']
                exec_time=l['Executor Run Time']
                write_time=math.floor(l['Shuffle Write Time']/1000000)
                ser_time=l['Result Serialization Time']
                getrst_time=l['Getting Result Time']
                durtime=fintime-sstime-starttime;

                times=[0,des_time,read_time,exec_time,write_time,ser_time,getrst_time]
                time_names=['sched delay','deserialize time','read time','executor time','write time','serialize time','result time']
                evttime=reduce((lambda x, y: x + y),times)
                if evttime>durtime:
                    times=[math.floor(l*1.0*durtime/evttime) for l in times]
                else:
                    times[0]=durtime-evttime

                esstime=sstime
                for idx in range(0,len(times)):
                    if times[idx]>0:
                        trace_events.append({
                             'tid':pid+int(t),
                             'ts':esstime,
                             'dur':times[idx],                
                             'pid':pid,
                             'ph':'X',
                             'name':time_names[idx]})
                        if idx==3:
                            trace_events.append({
                                 'tid':pid+int(t),
                                 'ts':esstime,
                                 'dur':l['JVM GC Time'],
                                 'pid':pid,
                                 'ph':'X',
                                 'name':'GC Time'})
                            if showcpu:
                                trace_events.append({
                                     'tid':pid+int(t),
                                     'ts':esstime,
                                     'pid':pid,
                                     'ph':'C',
                                     'name':'cpu% {:d}'.format(pid+int(t)),
                                     'args':{'value':l['Executor CPU Time']/1000000.0/times[idx]}})
                                trace_events.append({
                                     'tid':pid+int(t),
                                     'ts':esstime+times[idx],
                                     'pid':pid,
                                     'ph':'C',
                                     'name':'cpu% {:d}'.format(pid+int(t)),
                                     'args':{'value':0}})
                        esstime=esstime+times[idx]
        self.starttime=starttime
        return [json.dumps(l) for l in trace_events]

    def generate_trace_view(self,showcpu=False):
        traces=[]
        traces.extend(self.generate_log_trace_view(showcpu))
        
        output='''
        {
            "traceEvents": [
        
        ''' + \
        ",\n".join(traces)\
       + '''
            ]
        }'''

        with open('/home/yuzhou/catapult-master/tracing/test_data/'+self.appid+'.json', 'w') as outfile:  
            outfile.write(output)

        print("http://sr525:1088/tracing_examples/trace_viewer.html#/tracing/test_data/"+self.appid+".json")

    def show_Stage_histogram(apps,stageid,bincount):
        if self.selectjob is None:
            self.load_log()
        stage37=apps.selectjob.where("`Stage ID`={:d} and event='SparkListenerTaskEnd'".format(stageid) ).select(F.round((F.col('Finish Time')/1000-F.col('Launch Time')/1000),2).alias('elapsedtime'),F.round((F.col('`Bytes Read`')+F.col('`Local Bytes Read`')+F.col('`Remote Bytes Read`'))/1024/1024,2).alias('input'))
        hist_elapsedtime=stage37.select('elapsedtime').rdd.flatMap(lambda x: x).histogram(15)
        hist_input=stage37.select('input').rdd.flatMap(lambda x: x).histogram(15)
        fig, axs = plt.subplots(figsize=(30, 5),nrows=1, ncols=2)
        ax=axs[0]
        binSides, binCounts = hist_elapsedtime
        binSides=[builtins.round(l,2) for l in binSides]

        N = len(binCounts)
        ind = numpy.arange(N)
        width = 0.5

        rects1 = ax.bar(ind+0.5, binCounts, width, color='b')

        ax.set_ylabel('Frequencies')
        ax.set_title('stage{:d} elapsed time breakdown'.format(stageid))
        ax.set_xticks(numpy.arange(N+1))
        ax.set_xticklabels(binSides)

        ax=axs[1]
        binSides, binCounts = hist_input
        binSides=[builtins.round(l,2) for l in binSides]

        N = len(binCounts)
        ind = numpy.arange(N)
        width = 0.5
        rects1 = ax.bar(ind+0.5, binCounts, width, color='b')

        ax.set_ylabel('Frequencies')
        ax.set_title('stage{:d} input data breakdown'.format(stageid))
        ax.set_xticks(numpy.arange(N+1))
        ax.set_xticklabels(binSides)

        #ax.xaxis.set_major_formatter(mtick.FormatStrFormatter(''))
        #ax.yaxis.set_major_formatter(mtick.FormatStrFormatter(''))

        plt.show()
        
    def show_Stages_hist(apps,bincount=15,threshold=0.9):
        if self.selectjob is None:
            self.load_log()
        totaltime=apps.selectjob.where("event='SparkListenerTaskEnd'" ).agg(F.sum(F.col('Finish Time')-F.col('Launch Time')).alias('total_time')).collect()[0]['total_time']
        stage_time=apps.selectjob.where("event='SparkListenerTaskEnd'" ).groupBy('`Stage ID`').agg(F.sum(F.col('Finish Time')-F.col('Launch Time')).alias('total_time')).orderBy('total_time', ascending=False).toPandas()
        stage_time['acc_total'] = stage_time['total_time'].cumsum()/totaltime
        stage_time=stage_time.reset_index()
        fig, ax = plt.subplots(figsize=(30, 5))

        rects1 = ax.plot(stage_time['index'],stage_time['acc_total'],'b.-')
        ax.set_xticks(stage_time['index'])
        ax.set_xticklabels(stage_time['Stage ID'])
        ax.set_xlabel('stage')
        ax.grid(which='major', axis='x')
        plt.show()
        shownstage=stage_time.loc[stage_time['acc_total'] <=threshold]
        if len(shownstage)==0:
            shownstage=stage_time.loc[:0]
        for row in shownstage.itertuples(index=True):
            apps.show_Stage_histogram(getattr(row, "_2"),bincount) 
            
    def scatter_elapsetime_input(apps,stageid):
        if self.selectjob is None:
            self.load_log()
        stage37=apps.selectjob.where("`Stage ID`={:d} and event='SparkListenerTaskEnd'".format(stageid) ).select(F.round((F.col('Finish Time')/1000-F.col('Launch Time')/1000),2).alias('elapsedtime'),F.round((F.col('`Bytes Read`')+F.col('`Local Bytes Read`')+F.col('`Remote Bytes Read`'))/1024/1024,2).alias('input')).toPandas()
        stage37.plot.scatter('input','elapsedtime',figsize=(30, 5))

        
class Emon_Analysis:
    def __init__(self,emon_file, sc):
        self.emon_file=emon_file
        self.start_time=0  #time should be unix_time_stamp
        self.emon_metrics=collections.OrderedDict({
            'emon_ipc':{
                'sum_func':cores_sum,   
                'events':{
                    'a':'CPU_CLK_UNHALTED.THREAD',
                    'b':'INST_RETIRED.ANY'
                },
                'formula':{
                    'ipc':'b/a'
                },
                'fmt':lambda l: F.round(l, 3)
            },
            'emon_L3 stall':{'sum_func':cores_sum,   'events':{'a':'CYCLE_ACTIVITY.STALLS_L3_MISS','b':'CPU_CLK_UNHALTED.THREAD'},'formula':{'l3 stall':'a/b'},                         'fmt':lambda l: F.round(l, 3)},
            'emon_mem_bw':  {'sum_func':mem_sum,     'events':{'a':'UNC_M_CAS_COUNT.RD','b':'UNC_M_CAS_COUNT.WR'},                'formula':{'mem_bw_rd':'a*64/1000000','mem_bw_wr':'b*64/1000000'},   'fmt':lambda l: F.round(l, 0)},
            'emon_L3 lat':  {'sum_func':cores_sum,   'events':{'a':'UNC_CHA_TOR_OCCUPANCY.IA_MISS:filter1=0x40433','b':'UNC_CHA_TOR_INSERTS.IA_MISS:filter1=0x40433'},  'formula':{'mem_lat':'a/b'},   'fmt':lambda l: F.round(l, 1)},
            'emon_fp_vec':  {'sum_func':cores_sum,   'events':{'a':'FP_ARITH_INST_RETIRED.VECTOR','b':'INST_RETIRED.ANY'},  'formula':{'fp_vec/all inst':'a/b'},   'fmt':lambda l: F.round(l, 3)},
            'emon_l3 miss':  {'sum_func':cores_sum,   'events':{'a':'MEM_LOAD_RETIRED.L3_MISS','b':'INST_RETIRED.ANY'},  'formula':{'l3 miss per 1K inst':'1000*a/b'},   'fmt':lambda l: F.round(l, 3)},
            'emon_faststring bw':  {'sum_func':cores_sum,   'events':{'a':'UNC_CHA_TOR_INSERTS.IA_MISS:filter1=0x49033'},  'formula':{'fast store bw':'a*64/1000000'},   'fmt':lambda l: F.round(l, 0)},
            'emon_streaming bw':  {'sum_func':cores_sum,   'events':{'a':'UNC_CHA_TOR_INSERTS.IA_MISS:filter1=0x41833'},  'formula':{'streaming store bw':'a*64/1000000'},   'fmt':lambda l: F.round(l, 0)},
                         'metric_CPI':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'CPU_CLK_UNHALTED.THREAD','b':'INST_RETIRED.ANY'
                                },
                                'formula':{
                                       'metric_CPI':'a/b'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_branch mispredict ratio':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'BR_MISP_RETIRED.ALL_BRANCHES','b':'BR_INST_RETIRED.ALL_BRANCHES'
                                },
                                'formula':{
                                       'metric_branch mispredict ratio':'a/b'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_loads per instr':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'MEM_INST_RETIRED.ALL_LOADS','b':'INST_RETIRED.ANY'
                                },
                                'formula':{
                                       'metric_loads per instr':'a/b'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_stores per instr':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'MEM_INST_RETIRED.ALL_STORES','b':'INST_RETIRED.ANY'
                                },
                                'formula':{
                                       'metric_stores per instr':'a/b'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_locks retired per instr':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'MEM_INST_RETIRED.LOCK_LOADS','b':'INST_RETIRED.ANY'
                                },
                                'formula':{
                                       'metric_locks retired per instr':'a/b'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_uncacheable reads per instr':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_CHA_TOR_INSERTS.IA_MISS:filter1=0x40e33','c':'INST_RETIRED.ANY'
                                },
                                'formula':{
                                       'metric_uncacheable reads per instr':'a/c'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_streaming stores (full line) per instr':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_CHA_TOR_INSERTS.IA_MISS:filter1=0x41833','b':'INST_RETIRED.ANY'
                                },
                                'formula':{
                                       'metric_streaming stores (full line) per instr':'a/b'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_streaming stores (partial line) per instr':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_CHA_TOR_INSERTS.IA_MISS:filter1=0x41a33','b':'INST_RETIRED.ANY'
                                },
                                'formula':{
                                       'metric_streaming stores (partial line) per instr':'a/b'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_L1D MPI (includes data+rfo w/ prefetches)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'L1D.REPLACEMENT','b':'INST_RETIRED.ANY'
                                },
                                'formula':{
                                       'metric_L1D MPI (includes data+rfo w/ prefetches)':'a/b'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_L1D demand data read hits per instr':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'MEM_LOAD_RETIRED.L1_HIT','b':'INST_RETIRED.ANY'
                                },
                                'formula':{
                                       'metric_L1D demand data read hits per instr':'a/b'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_L1-I code read misses (w/ prefetches) per instr':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'L2_RQSTS.ALL_CODE_RD','b':'INST_RETIRED.ANY'
                                },
                                'formula':{
                                       'metric_L1-I code read misses (w/ prefetches) per instr':'a/b'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_L2 demand data read hits per instr':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'MEM_LOAD_RETIRED.L2_HIT','b':'INST_RETIRED.ANY'
                                },
                                'formula':{
                                       'metric_L2 demand data read hits per instr':'a/b'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_L2 MPI (includes code+data+rfo w/ prefetches)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'L2_LINES_IN.ALL','b':'INST_RETIRED.ANY'
                                },
                                'formula':{
                                       'metric_L2 MPI (includes code+data+rfo w/ prefetches)':'a/b'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_L2 demand data read MPI':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'MEM_LOAD_RETIRED.L2_MISS','b':'INST_RETIRED.ANY'
                                },
                                'formula':{
                                       'metric_L2 demand data read MPI':'a/b'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_L2 demand code MPI':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'L2_RQSTS.CODE_RD_MISS','b':'INST_RETIRED.ANY'
                                },
                                'formula':{
                                       'metric_L2 demand code MPI':'a/b'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_L2 Any local request that HITM in a sibling core (per instr)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'OFFCORE_RESPONSE:request=ALL_READS:response=L3_HIT.HITM_OTHER_CORE','c':'INST_RETIRED.ANY'
                                },
                                'formula':{
                                       'metric_L2 Any local request that HITM in a sibling core (per instr)':'a/c'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_L2 Any local request that HIT in a sibling core and forwarded(per instr)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'OFFCORE_RESPONSE:request=ALL_READS:response=L3_HIT.HIT_OTHER_CORE_FWD','c':'INST_RETIRED.ANY'
                                },
                                'formula':{
                                       'metric_L2 Any local request that HIT in a sibling core and forwarded(per instr)':'a/c'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_L2 all L2 prefetches(per instr)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'L2_RQSTS.ALL_PF','c':'INST_RETIRED.ANY'
                                },
                                'formula':{
                                       'metric_L2 all L2 prefetches(per instr)':'a/c'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_L2 % of L2 evictions that are allocated into L3':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'L2_LINES_OUT.NON_SILENT','b':'IDI_MISC.WB_DOWNGRADE'
                                },
                                'formula':{
                                       'metric_L2 % of L2 evictions that are allocated into L3':'100*(a-b)/a'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_L2 % of L2 evictions that are NOT allocated into L3':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'L2_LINES_OUT.NON_SILENT','b':'IDI_MISC.WB_DOWNGRADE'
                                },
                                'formula':{
                                       'metric_L2 % of L2 evictions that are NOT allocated into L3':'100*b/a'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_LLC code references per instr (L3 prefetch excluded)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_CHA_TOR_INSERTS.IA:filter1=0x40233','d':'INST_RETIRED.ANY'
                                },
                                'formula':{
                                       'metric_LLC code references per instr (L3 prefetch excluded)':'a/d'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_LLC data read references per instr (L3 prefetch excluded)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_CHA_TOR_INSERTS.IA:filter1=0x40433','d':'INST_RETIRED.ANY'
                                },
                                'formula':{
                                       'metric_LLC data read references per instr (L3 prefetch excluded)':'a/d'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_LLC RFO references per instr (L3 prefetch excluded)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_CHA_TOR_INSERTS.IA:filter1=0x40033','d':'INST_RETIRED.ANY'
                                },
                                'formula':{
                                       'metric_LLC RFO references per instr (L3 prefetch excluded)':'a/d'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_LLC MPI (includes code+data+rfo w/ prefetches)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_CHA_TOR_INSERTS.IA_MISS:filter1=0x12D40433','b':'UNC_CHA_TOR_INSERTS.IA_MISS:filter1=0x12CC0233','c':'UNC_CHA_TOR_INSERTS.IA_MISS:filter1=0x12C40033','d':'INST_RETIRED.ANY'
                                },
                                'formula':{
                                       'metric_LLC MPI (includes code+data+rfo w/ prefetches)':'(a+b+c)/d'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_LLC data read MPI (demand+prefetch)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_CHA_TOR_INSERTS.IA_MISS:filter1=0x12D40433','b':'INST_RETIRED.ANY'
                                },
                                'formula':{
                                       'metric_LLC data read MPI (demand+prefetch)':'a/b'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_LLC RFO read MPI (demand+prefetch)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_CHA_TOR_INSERTS.IA_MISS:filter1=0x12C40033','b':'INST_RETIRED.ANY'
                                },
                                'formula':{
                                       'metric_LLC RFO read MPI (demand+prefetch)':'a/b'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_LLC code read MPI (demand+prefetch)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_CHA_TOR_INSERTS.IA_MISS:filter1=0x12CC0233','b':'INST_RETIRED.ANY'
                                },
                                'formula':{
                                       'metric_LLC code read MPI (demand+prefetch)':'a/b'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_LLC all LLC prefetches (per instr)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_CHA_TOR_INSERTS.IA_MISS:filter1=0x12C4B433','b':'INST_RETIRED.ANY'
                                },
                                'formula':{
                                       'metric_LLC all LLC prefetches (per instr)':'a/b'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_LLC total HITM (per instr) (excludes LLC prefetches)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'OFFCORE_RESPONSE:request=ALL_READS:response=L3_MISS.REMOTE_HITM','b':'INST_RETIRED.ANY'
                                },
                                'formula':{
                                       'metric_LLC total HITM (per instr) (excludes LLC prefetches)':'a/b'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_LLC total HIT clean line forwards (per instr) (excludes LLC prefetches)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'OFFCORE_RESPONSE:request=ALL_READS:response=L3_MISS.REMOTE_HIT_FORWARD','b':'INST_RETIRED.ANY'
                                },
                                'formula':{
                                       'metric_LLC total HIT clean line forwards (per instr) (excludes LLC prefetches)':'a/b'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_LLC % of LLC misses satisfied by remote caches':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_CHA_TOR_INSERTS.IA_MISS:filter1=0x12D40433','b':'UNC_CHA_TOR_INSERTS.IA_MISS:filter1=0x12CC0233','c':'UNC_CHA_TOR_INSERTS.IA_MISS:filter1=0x12C40033','d':'UNC_CHA_TOR_INSERTS.IA_MISS:filter1=0x12C4B433','e':'OFFCORE_RESPONSE:request=ALL_READS:response=L3_MISS.REMOTE_HIT_FORWARD','f':'OFFCORE_RESPONSE:request=ALL_READS:response=L3_MISS.REMOTE_HITM'
                                },
                                'formula':{
                                       'metric_LLC % of LLC misses satisfied by remote caches':'100*(e+f)/(a+b+c-d)'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_SF snoop filter capacity evictions (per instr)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_CHA_SF_EVICTION.M_STATE','b':'UNC_CHA_SF_EVICTION.S_STATE','c':'UNC_CHA_SF_EVICTION.E_STATE','d':'INST_RETIRED.ANY'
                                },
                                'formula':{
                                       'metric_SF snoop filter capacity evictions (per instr)':'(a+b+c)/d'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_SF % of L3 accesses that result in SF capacity evictions':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_CHA_SF_EVICTION.M_STATE','b':'UNC_CHA_SF_EVICTION.S_STATE','c':'UNC_CHA_SF_EVICTION.E_STATE','d':'L2_LINES_IN.ALL'
                                },
                                'formula':{
                                       'metric_SF % of L3 accesses that result in SF capacity evictions':'100*(a+b+c)/d'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_ITLB MPI':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'ITLB_MISSES.WALK_COMPLETED','b':'INST_RETIRED.ANY'
                                },
                                'formula':{
                                       'metric_ITLB MPI':'a/b'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_ITLB large page MPI':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'ITLB_MISSES.WALK_COMPLETED_2M_4M','b':'INST_RETIRED.ANY'
                                },
                                'formula':{
                                       'metric_ITLB large page MPI':'a/b'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_DTLB load MPI':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'DTLB_LOAD_MISSES.WALK_COMPLETED','b':'INST_RETIRED.ANY'
                                },
                                'formula':{
                                       'metric_DTLB load MPI':'a/b'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_DTLB 4KB page load MPI':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'DTLB_LOAD_MISSES.WALK_COMPLETED','b':'DTLB_LOAD_MISSES.WALK_COMPLETED_2M_4M','c':'DTLB_LOAD_MISSES.WALK_COMPLETED_1G','d':'INST_RETIRED.ANY'
                                },
                                'formula':{
                                       'metric_DTLB 4KB page load MPI':'(a-b-c)/d'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_DTLB 2MB large page load MPI':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'DTLB_LOAD_MISSES.WALK_COMPLETED_2M_4M','b':'INST_RETIRED.ANY'
                                },
                                'formula':{
                                       'metric_DTLB 2MB large page load MPI':'a/b'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_DTLB 1GB large page load MPI':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'DTLB_LOAD_MISSES.WALK_COMPLETED_1G','b':'INST_RETIRED.ANY'
                                },
                                'formula':{
                                       'metric_DTLB 1GB large page load MPI':'a/b'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_DTLB store MPI':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'DTLB_STORE_MISSES.WALK_COMPLETED','b':'INST_RETIRED.ANY'
                                },
                                'formula':{
                                       'metric_DTLB store MPI':'a/b'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_DTLB load miss latency (in core clks)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'DTLB_LOAD_MISSES.WALK_ACTIVE','b':'DTLB_LOAD_MISSES.WALK_COMPLETED'
                                },
                                'formula':{
                                       'metric_DTLB load miss latency (in core clks)':'a/b'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_DTLB store miss latency (in core clks)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'DTLB_STORE_MISSES.WALK_ACTIVE','b':'DTLB_STORE_MISSES.WALK_COMPLETED'
                                },
                                'formula':{
                                       'metric_DTLB store miss latency (in core clks)':'a/b'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_ITLB miss latency (in core clks)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'ITLB_MISSES.WALK_ACTIVE','b':'ITLB_MISSES.WALK_COMPLETED'
                                },
                                'formula':{
                                       'metric_ITLB miss latency (in core clks)':'a/b'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_NUMA %_Reads addressed to local DRAM':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_CHA_TOR_INSERTS.IA_MISS:filter1=0x40432','b':'UNC_CHA_TOR_INSERTS.IA_MISS:filter1=0x40431'
                                },
                                'formula':{
                                       'metric_NUMA %_Reads addressed to local DRAM':'100*a/(a+b)'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_NUMA %_Reads addressed to remote DRAM':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_CHA_TOR_INSERTS.IA_MISS:filter1=0x40432','b':'UNC_CHA_TOR_INSERTS.IA_MISS:filter1=0x40431'
                                },
                                'formula':{
                                       'metric_NUMA %_Reads addressed to remote DRAM':'100*b/(a+b)'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_NUMA %_RFOs addressed to local DRAM':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_CHA_TOR_INSERTS.IA_MISS:filter1=0x12C40031','b':'UNC_CHA_TOR_INSERTS.IA_MISS:filter1=0x12C40033'
                                },
                                'formula':{
                                       'metric_NUMA %_RFOs addressed to local DRAM':'100*(b-a)/b'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_NUMA %_RFOs addressed to remote DRAM':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_CHA_TOR_INSERTS.IA_MISS:filter1=0x12C40031','b':'UNC_CHA_TOR_INSERTS.IA_MISS:filter1=0x12C40033'
                                },
                                'formula':{
                                       'metric_NUMA %_RFOs addressed to remote DRAM':'100*a/b'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_UPI Data transmit BW (MB/sec) (only data)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_UPI_TxL_FLITS.ALL_DATA'
                                },
                                'formula':{
                                       'metric_UPI Data transmit BW (MB/sec) (only data)':'a*(64/9)/1000000'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_UPI % cycles transmit link is half-width (L0p)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_UPI_TxL0P_POWER_CYCLES','b':'UNC_UPI_CLOCKTICKS','f':'UNC_UPI_L1_POWER_CYCLES'
                                },
                                'formula':{
                                       'metric_UPI % cycles transmit link is half-width (L0p)':'100*(a/(b-f))'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_UPI % cycles receive link is half-width (L0p)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_UPI_RxL0P_POWER_CYCLES','b':'UNC_UPI_CLOCKTICKS','f':'UNC_UPI_L1_POWER_CYCLES'
                                },
                                'formula':{
                                       'metric_UPI % cycles receive link is half-width (L0p)':'100*(a/(b-f))'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_HA - Reads vs. all requests':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_CHA_REQUESTS.READS','b':'UNC_CHA_REQUESTS.WRITES'
                                },
                                'formula':{
                                       'metric_HA - Reads vs. all requests':'a/(a+b)'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_HA - Writes vs. all requests':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_CHA_REQUESTS.READS','b':'UNC_CHA_REQUESTS.WRITES'
                                },
                                'formula':{
                                       'metric_HA - Writes vs. all requests':'b/(a+b)'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_HA % of all reads that are local':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_CHA_REQUESTS.READS_LOCAL','b':'UNC_CHA_REQUESTS.READS'
                                },
                                'formula':{
                                       'metric_HA % of all reads that are local':'100*a/b'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_HA % of all writes that are local':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_CHA_REQUESTS.WRITES_LOCAL','b':'UNC_CHA_REQUESTS.WRITES'
                                },
                                'formula':{
                                       'metric_HA % of all writes that are local':'100*a/b'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_HA conflict responses per instr':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_CHA_SNOOP_RESP.RSPCNFLCTS','b':'INST_RETIRED.ANY'
                                },
                                'formula':{
                                       'metric_HA conflict responses per instr':'a/b'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_HA directory lookups that spawned a snoop (per instr)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_CHA_DIR_LOOKUP.SNP','b':'INST_RETIRED.ANY'
                                },
                                'formula':{
                                       'metric_HA directory lookups that spawned a snoop (per instr)':'a/b'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_HA directory lookups that did not spawn a snoop (per instr)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_CHA_DIR_LOOKUP.NO_SNP','b':'INST_RETIRED.ANY'
                                },
                                'formula':{
                                       'metric_HA directory lookups that did not spawn a snoop (per instr)':'a/b'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_M2M directory updates (per instr)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_CHA_DIR_UPDATE.HA','b':'UNC_CHA_DIR_UPDATE.TOR','c':'UNC_M2M_DIRECTORY_UPDATE.ANY','d':'INST_RETIRED.ANY'
                                },
                                'formula':{
                                       'metric_M2M directory updates (per instr)':'(a+b+c)/d'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_M2M XPT prefetches (per instr)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_M2M_PREFCAM_INSERTS','b':'UNC_M3UPI_UPI_PREFETCH_SPAWN','c':'INST_RETIRED.ANY'
                                },
                                'formula':{
                                       'metric_M2M XPT prefetches (per instr)':'(a-b)/c'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_M3UPI UPI prefetches (per instr)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'b':'UNC_M3UPI_UPI_PREFETCH_SPAWN','c':'INST_RETIRED.ANY'
                                },
                                'formula':{
                                       'metric_M3UPI UPI prefetches (per instr)':'b/c'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_M2M extra reads from XPT-UPI prefetches (per instr)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_M2M_PREFCAM_INSERTS','b':'UNC_M2M_PREFCAM_DEMAND_PROMOTIONS','c':'INST_RETIRED.ANY'
                                },
                                'formula':{
                                       'metric_M2M extra reads from XPT-UPI prefetches (per instr)':'(a-b)/c'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_memory bandwidth read (MB/sec)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_M_CAS_COUNT.RD'
                                },
                                'formula':{
                                       'metric_memory bandwidth read (MB/sec)':'a*64/1000000'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_memory bandwidth write (MB/sec)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_M_CAS_COUNT.WR'
                                },
                                'formula':{
                                       'metric_memory bandwidth write (MB/sec)':'a*64/1000000'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_memory bandwidth total (MB/sec)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_M_CAS_COUNT.RD','b':'UNC_M_CAS_COUNT.WR'
                                },
                                'formula':{
                                       'metric_memory bandwidth total (MB/sec)':'(a+b)*64/1000000'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_memory extra read b/w due to XPT prefetches (MB/sec)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_M2M_PREFCAM_INSERTS','b':'UNC_M2M_PREFCAM_DEMAND_PROMOTIONS'
                                },
                                'formula':{
                                       'metric_memory extra read b/w due to XPT prefetches (MB/sec)':'(a-b)*64/1000000'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_memory extra write b/w due to directory updates (MB/sec)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_CHA_DIR_UPDATE.HA','b':'UNC_CHA_DIR_UPDATE.TOR','c':'UNC_M2M_DIRECTORY_UPDATE.ANY'
                                },
                                'formula':{
                                       'metric_memory extra write b/w due to directory updates (MB/sec)':'(a+b+c)*64/1000000'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_2LM % of non-inclusive writes to near memory':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_M2M_IMC_WRITES.NI','b':'UNC_M_DDRT_RDQ_INSERTS','c':'UNC_M_CAS_COUNT.WR'
                                },
                                'formula':{
                                       'metric_2LM % of non-inclusive writes to near memory':'100*a/(c-b)'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_2LM near memory cache read miss rate%':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_M2M_TAG_HIT.NM_RD_HIT_CLEAN','b':'UNC_M2M_TAG_HIT.NM_RD_HIT_DIRTY','c':'UNC_M_DDRT_RDQ_INSERTS'
                                },
                                'formula':{
                                       'metric_2LM near memory cache read miss rate%':'100*c/(a+b+c)'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_memory avg entries in RPQ':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_M_CLOCKTICKS','c':'UNC_M_RPQ_OCCUPANCY'
                                },
                                'formula':{
                                       'metric_memory avg entries in RPQ':'c/a'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_memory avg entries in RPQ when not empty':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'c':'UNC_M_RPQ_OCCUPANCY','a':'UNC_M_RPQ_OCCUPANCY:t=1'
                                },
                                'formula':{
                                       'metric_memory avg entries in RPQ when not empty':'c/a'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_memory % cycles when RPQ is empty':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_M_CLOCKTICKS','c':'UNC_M_RPQ_OCCUPANCY:t=1'
                                },
                                'formula':{
                                       'metric_memory % cycles when RPQ is empty':'100*(1-c/a)'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_memory % cycles when RPQ has 1 or more entries':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_M_CLOCKTICKS','c':'UNC_M_RPQ_OCCUPANCY:t=1'
                                },
                                'formula':{
                                       'metric_memory % cycles when RPQ has 1 or more entries':'100*c/a'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_memory % cycles when RPQ has 10 or more entries':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_M_CLOCKTICKS','c':'UNC_M_RPQ_OCCUPANCY:t=10'
                                },
                                'formula':{
                                       'metric_memory % cycles when RPQ has 10 or more entries':'100*c/a'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_memory % cycles when RPQ has 20 or more entries':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_M_CLOCKTICKS','c':'UNC_M_RPQ_OCCUPANCY:t=20'
                                },
                                'formula':{
                                       'metric_memory % cycles when RPQ has 20 or more entries':'100*c/a'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_memory % cycles when RPQ has 40 or more entries':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_M_CLOCKTICKS','c':'UNC_M_RPQ_OCCUPANCY:t=40'
                                },
                                'formula':{
                                       'metric_memory % cycles when RPQ has 40 or more entries':'100*c/a'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_memory avg time (dclk) RPQ not empty':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_M_RPQ_OCCUPANCY:t=1:e1','c':'UNC_M_RPQ_OCCUPANCY:t=1'
                                },
                                'formula':{
                                       'metric_memory avg time (dclk) RPQ not empty':'c/a'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_memory avg time (dclk) RPQ empty':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_M_RPQ_OCCUPANCY:t=1:e1','c':'UNC_M_RPQ_OCCUPANCY:t=1','d':'UNC_M_CLOCKTICKS'
                                },
                                'formula':{
                                       'metric_memory avg time (dclk) RPQ empty':'(d-c)/a'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_memory avg time with 40 or more entries':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_M_RPQ_OCCUPANCY:t=40:e1','c':'UNC_M_RPQ_OCCUPANCY:t=40'
                                },
                                'formula':{
                                       'metric_memory avg time with 40 or more entries':'c/a'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_memory avg time with less than 40 entries)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_M_RPQ_OCCUPANCY:t=40:e1','c':'UNC_M_RPQ_OCCUPANCY:t=40','d':'UNC_M_CLOCKTICKS'
                                },
                                'formula':{
                                       'metric_memory avg time with less than 40 entries)':'(d-c)/a'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_3DXP_memory bandwidth read (MB/sec)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_M_DDRT_RDQ_INSERTS'
                                },
                                'formula':{
                                       'metric_3DXP_memory bandwidth read (MB/sec)':'a*64/1000000'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_3DXP_memory bandwidth write (MB/sec)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_M_DDRT_WPQ_INSERTS'
                                },
                                'formula':{
                                       'metric_3DXP_memory bandwidth write (MB/sec)':'a*64/1000000'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_3DXP_memory bandwidth total (MB/sec)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_M_DDRT_RDQ_INSERTS','b':'UNC_M_DDRT_WPQ_INSERTS'
                                },
                                'formula':{
                                       'metric_3DXP_memory bandwidth total (MB/sec)':'(a+b)*64/1000000'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_3DXP avg entries in RPQ when not empty':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'b':'UNC_M_DDRT_RDQ_OCCUPANCY.ALL:t=1','c':'UNC_M_DDRT_RDQ_OCCUPANCY.ALL'
                                },
                                'formula':{
                                       'metric_3DXP avg entries in RPQ when not empty':'c/b'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_3DXP % cycles when RPQ is empty':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_M_CLOCKTICKS','b':'UNC_M_DDRT_RDQ_OCCUPANCY.ALL:t=1'
                                },
                                'formula':{
                                       'metric_3DXP % cycles when RPQ is empty':'100*(1-(b/a))'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_3DXP avg time (dclk) RPQ not empty':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_M_DDRT_RDQ_OCCUPANCY.ALL:t=1:e1','b':'UNC_M_DDRT_RDQ_OCCUPANCY.ALL:t=1'
                                },
                                'formula':{
                                       'metric_3DXP avg time (dclk) RPQ not empty':'b/a'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_3DXP avg time (dclk) with 36 or more entries in RPQ':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_M_DDRT_RDQ_OCCUPANCY.ALL:t=36:e1','b':'UNC_M_DDRT_RDQ_OCCUPANCY.ALL:t=36'
                                },
                                'formula':{
                                       'metric_3DXP avg time (dclk) with 36 or more entries in RPQ':'b/a'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_3DXP avg entries in WPQ when not empty':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'b':'UNC_M_DDRT_WPQ_OCCUPANCY.ALL:t=1','c':'UNC_M_DDRT_WPQ_OCCUPANCY.ALL'
                                },
                                'formula':{
                                       'metric_3DXP avg entries in WPQ when not empty':'c/b'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_3DXP avg time (dclk) WPQ not empty':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_M_DDRT_WPQ_OCCUPANCY.ALL:t=1:e1','b':'UNC_M_DDRT_WPQ_OCCUPANCY.ALL:t=1'
                                },
                                'formula':{
                                       'metric_3DXP avg time (dclk) WPQ not empty':'b/a'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_3DXP avg time (dclk) with 30 or more entries in WPQ':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_M_DDRT_WPQ_OCCUPANCY.ALL:t=30:e1','b':'UNC_M_DDRT_WPQ_OCCUPANCY.ALL:t=30'
                                },
                                'formula':{
                                       'metric_3DXP avg time (dclk) with 30 or more entries in WPQ':'b/a'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_CHA % cyles Fast asserted':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_CHA_FAST_ASSERTED.HORZ:u0x1','c':'UNC_CHA_CLOCKTICKS'
                                },
                                'formula':{
                                       'metric_CHA % cyles Fast asserted':'100*a/c'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_CHA RxC IRQ avg entries':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_CHA_RxC_OCCUPANCY.IRQ','c':'UNC_CHA_CLOCKTICKS'
                                },
                                'formula':{
                                       'metric_CHA RxC IRQ avg entries':'a/c'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_CHA RxC IRQ % cycles when Q has 18 or more entries':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_CHA_CLOCKTICKS','c':'UNC_CHA_RxC_OCCUPANCY.IRQ:t=18'
                                },
                                'formula':{
                                       'metric_CHA RxC IRQ % cycles when Q has 18 or more entries':'100*c/a'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_M2M avg entries in TxC AD Q':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_CHA_CLOCKTICKS','c':'UNC_M2M_TxC_AD_OCCUPANCY'
                                },
                                'formula':{
                                       'metric_M2M avg entries in TxC AD Q':'c/a'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_M2M avg entries in TxC BL Q':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_CHA_CLOCKTICKS','c':'UNC_M2M_TxC_BL_OCCUPANCY.ALL'
                                },
                                'formula':{
                                       'metric_M2M avg entries in TxC BL Q':'c/a'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_M2M RxC AD avg entries':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_M2M_RxC_AD_OCCUPANCY','c':'UNC_CHA_CLOCKTICKS'
                                },
                                'formula':{
                                       'metric_M2M RxC AD avg entries':'a/c'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_M2M RxC BL avg entries':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_M2M_RxC_BL_OCCUPANCY','c':'UNC_CHA_CLOCKTICKS'
                                },
                                'formula':{
                                       'metric_M2M RxC BL avg entries':'a/c'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_IO_bandwidth_disk_or_network_writes (MB/sec)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_IIO_PAYLOAD_BYTES_IN.MEM_READ.PART0','b':'UNC_IIO_PAYLOAD_BYTES_IN.MEM_READ.PART1','c':'UNC_IIO_PAYLOAD_BYTES_IN.MEM_READ.PART2','d':'UNC_IIO_PAYLOAD_BYTES_IN.MEM_READ.PART3'
                                },
                                'formula':{
                                       'metric_IO_bandwidth_disk_or_network_writes (MB/sec)':'(a+b+c+d)*4/1000000'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_IO_bandwidth_disk_or_network_reads (MB/sec)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_IIO_PAYLOAD_BYTES_IN.MEM_WRITE.PART0','b':'UNC_IIO_PAYLOAD_BYTES_IN.MEM_WRITE.PART1','c':'UNC_IIO_PAYLOAD_BYTES_IN.MEM_WRITE.PART2','d':'UNC_IIO_PAYLOAD_BYTES_IN.MEM_WRITE.PART3'
                                },
                                'formula':{
                                       'metric_IO_bandwidth_disk_or_network_reads (MB/sec)':'(a+b+c+d)*4/1000000'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_IO_number of partial PCI writes per sec':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_CHA_TOR_INSERTS.IO_HIT:filter1=0x40033','b':'UNC_CHA_TOR_INSERTS.IO_MISS:filter1=0x40033'
                                },
                                'formula':{
                                       'metric_IO_number of partial PCI writes per sec':'a+b'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_IO_read cache miss(disk/network writes) bandwidth (MB/sec)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_CHA_TOR_INSERTS.IO_MISS:filter1=0x43c33'
                                },
                                'formula':{
                                       'metric_IO_read cache miss(disk/network writes) bandwidth (MB/sec)':'a*64/1000000'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_IO_write cache miss(disk/network reads) bandwidth (MB/sec)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_CHA_TOR_INSERTS.IO_MISS:filter1=0x49033','b':'UNC_CHA_TOR_INSERTS.IO_MISS:filter1=0x40033'
                                },
                                'formula':{
                                       'metric_IO_write cache miss(disk/network reads) bandwidth (MB/sec)':'(a+b)*64/1000000'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_IONUMA % disk/network reads addressed to local memory':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_CHA_TOR_INSERTS.IO:filter1=0x49031','b':'UNC_CHA_TOR_INSERTS.IO:filter1=0x49032','c':'UNC_CHA_TOR_INSERTS.IO:filter1=0x40031','d':'UNC_CHA_TOR_INSERTS.IO:filter1=0x40032'
                                },
                                'formula':{
                                       'metric_IONUMA % disk/network reads addressed to local memory':'100*(b+d)/(a+b+c+d)'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_IONUMA % disk/network reads addressed to remote memory':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_CHA_TOR_INSERTS.IO:filter1=0x49031','b':'UNC_CHA_TOR_INSERTS.IO:filter1=0x49032','c':'UNC_CHA_TOR_INSERTS.IO:filter1=0x40031','d':'UNC_CHA_TOR_INSERTS.IO:filter1=0x40032'
                                },
                                'formula':{
                                       'metric_IONUMA % disk/network reads addressed to remote memory':'100*(a+c)/(a+b+c+d)'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_MMIO reads per instr':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_CHA_TOR_INSERTS.IA_MISS:filter1=0x40040e33','b':'INST_RETIRED.ANY'
                                },
                                'formula':{
                                       'metric_MMIO reads per instr':'a/b'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_MMIO writes per instr':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_CHA_TOR_INSERTS.IA_MISS:filter1=0x40041e33','b':'INST_RETIRED.ANY'
                                },
                                'formula':{
                                       'metric_MMIO writes per instr':'a/b'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_memory reads vs. all requests':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_M_CAS_COUNT.RD','b':'UNC_M_CAS_COUNT.WR'
                                },
                                'formula':{
                                       'metric_memory reads vs. all requests':'a/(a+b)'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_memory Page Empty vs. all requests':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_M_PRE_COUNT.RD:u0xc','c':'UNC_M_PRE_COUNT.PAGE_MISS','d':'UNC_M_CAS_COUNT.RD','e':'UNC_M_CAS_COUNT.WR'
                                },
                                'formula':{
                                       'metric_memory Page Empty vs. all requests':'(a-c)/(d+e)'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_memory Page Misses vs. all requests':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'b':'UNC_M_PRE_COUNT.PAGE_MISS','c':'UNC_M_CAS_COUNT.RD','d':'UNC_M_CAS_COUNT.WR'
                                },
                                'formula':{
                                       'metric_memory Page Misses vs. all requests':'b/(c+d)'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_memory Page Hits vs. all requests':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_M_PRE_COUNT.RD:u0xc','c':'UNC_M_CAS_COUNT.RD','d':'UNC_M_CAS_COUNT.WR'
                                },
                                'formula':{
                                       'metric_memory Page Hits vs. all requests':'1-(a/(c+d))'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_memory % Cycles where all DRAM ranks are in PPD mode':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_M_POWER_CHANNEL_PPD','b':'UNC_M_CLOCKTICKS'
                                },
                                'formula':{
                                       'metric_memory % Cycles where all DRAM ranks are in PPD mode':'100*a/b'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_memory % Cycles Memory is in self refresh power mode':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_M_POWER_SELF_REFRESH','b':'UNC_M_CLOCKTICKS'
                                },
                                'formula':{
                                       'metric_memory % Cycles Memory is in self refresh power mode':'100*a/b'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_ItoM operations (fast strings) that reference LLC per instr':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_CHA_TOR_INSERTS.IA_HIT:filter1=0x49033','b':'UNC_CHA_TOR_INSERTS.IA_MISS:filter1=0x49033','c':'INST_RETIRED.ANY'
                                },
                                'formula':{
                                       'metric_ItoM operations (fast strings) that reference LLC per instr':'(a+b)/c'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_ItoM operations (fast strings) that miss LLC per instr':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_CHA_TOR_INSERTS.IA_MISS:filter1=0x49033','c':'INST_RETIRED.ANY'
                                },
                                'formula':{
                                       'metric_ItoM operations (fast strings) that miss LLC per instr':'a/c'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_% Uops delivered from decoded Icache (DSB)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'IDQ.DSB_UOPS','b':'UOPS_ISSUED.ANY'
                                },
                                'formula':{
                                       'metric_% Uops delivered from decoded Icache (DSB)':'100*(a/b)'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_% Uops delivered from legacy decode pipeline (MITE)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'IDQ.MITE_UOPS','b':'UOPS_ISSUED.ANY'
                                },
                                'formula':{
                                       'metric_% Uops delivered from legacy decode pipeline (MITE)':'100*(a/b)'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_% Uops delivered from microcode sequencer (MS)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'IDQ.MS_UOPS','b':'UOPS_ISSUED.ANY'
                                },
                                'formula':{
                                       'metric_% Uops delivered from microcode sequencer (MS)':'100*(a/b)'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_% Uops delivered from loop stream detector (LSD)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'LSD.UOPS','b':'UOPS_ISSUED.ANY'
                                },
                                'formula':{
                                       'metric_% Uops delivered from loop stream detector (LSD)':'100*(a/b)'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_FP scalar single-precision FP instructions retired per instr':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'FP_ARITH_INST_RETIRED.SCALAR_SINGLE','b':'INST_RETIRED.ANY'
                                },
                                'formula':{
                                       'metric_FP scalar single-precision FP instructions retired per instr':'a/b'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_FP scalar double-precision FP instructions retired per instr':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'FP_ARITH_INST_RETIRED.SCALAR_DOUBLE','b':'INST_RETIRED.ANY'
                                },
                                'formula':{
                                       'metric_FP scalar double-precision FP instructions retired per instr':'a/b'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_FP 128-bit packed single-precision FP instructions retired per instr':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'FP_ARITH_INST_RETIRED.128B_PACKED_SINGLE','b':'INST_RETIRED.ANY'
                                },
                                'formula':{
                                       'metric_FP 128-bit packed single-precision FP instructions retired per instr':'a/b'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_FP 128-bit packed double-precision FP instructions retired per instr':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'FP_ARITH_INST_RETIRED.128B_PACKED_DOUBLE','b':'INST_RETIRED.ANY'
                                },
                                'formula':{
                                       'metric_FP 128-bit packed double-precision FP instructions retired per instr':'a/b'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_FP 256-bit packed single-precision FP instructions retired per instr':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'FP_ARITH_INST_RETIRED.256B_PACKED_SINGLE','b':'INST_RETIRED.ANY'
                                },
                                'formula':{
                                       'metric_FP 256-bit packed single-precision FP instructions retired per instr':'a/b'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_FP 256-bit packed double-precision FP instructions retired per instr':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'FP_ARITH_INST_RETIRED.256B_PACKED_DOUBLE','b':'INST_RETIRED.ANY'
                                },
                                'formula':{
                                       'metric_FP 256-bit packed double-precision FP instructions retired per instr':'a/b'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_FP 512-bit packed single-precision FP instructions retired per instr':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'FP_ARITH_INST_RETIRED.512B_PACKED_SINGLE','b':'INST_RETIRED.ANY'
                                },
                                'formula':{
                                       'metric_FP 512-bit packed single-precision FP instructions retired per instr':'a/b'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_FP 512-bit packed double-precision FP instructions retired per instr':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'FP_ARITH_INST_RETIRED.512B_PACKED_DOUBLE','b':'INST_RETIRED.ANY'
                                },
                                'formula':{
                                       'metric_FP 512-bit packed double-precision FP instructions retired per instr':'a/b'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_DRAM power (watts)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'MSR_EVENT:msr=0x619:type=FREERUN:scope=PACKAGE'
                                },
                                'formula':{
                                       'metric_DRAM power (watts)':'a*15.3/1000000'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_package power (watts)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'MSR_EVENT:msr=0x611:type=FREERUN:scope=PACKAGE'
                                },
                                'formula':{
                                       'metric_package power (watts)':'a*61/1000000'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_core c6 residency %':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'MSR_EVENT:msr=0x3FD:type=FREERUN:scope=THREAD','b':'TSC'
                                },
                                'formula':{
                                       'metric_core c6 residency %':'100*a/b'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_core SW prefetch NTA per instr':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'SW_PREFETCH_ACCESS.NTA','c':'INST_RETIRED.ANY'
                                },
                                'formula':{
                                       'metric_core SW prefetch NTA per instr':'a/c'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_core uncacheable access and clflushes per instr':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'OFFCORE_REQUESTS.MEM_UC','c':'INST_RETIRED.ANY'
                                },
                                'formula':{
                                       'metric_core uncacheable access and clflushes per instr':'a/c'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_core % cycles core power throttled':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'CORE_POWER.THROTTLE','c':'CPU_CLK_UNHALTED.THREAD'
                                },
                                'formula':{
                                       'metric_core % cycles core power throttled':'100*a/c'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_core % cycles in non AVX license':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'CORE_POWER.LVL0_TURBO_LICENSE','b':'CORE_POWER.LVL1_TURBO_LICENSE','c':'CORE_POWER.LVL2_TURBO_LICENSE'
                                },
                                'formula':{
                                       'metric_core % cycles in non AVX license':'100*a/(a+b+c)'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_core % cycles in AVX2 license':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'CORE_POWER.LVL0_TURBO_LICENSE','b':'CORE_POWER.LVL1_TURBO_LICENSE','c':'CORE_POWER.LVL2_TURBO_LICENSE'
                                },
                                'formula':{
                                       'metric_core % cycles in AVX2 license':'100*b/(a+b+c)'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_core % cycles in AVX-512 license':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'CORE_POWER.LVL0_TURBO_LICENSE','b':'CORE_POWER.LVL1_TURBO_LICENSE','c':'CORE_POWER.LVL2_TURBO_LICENSE'
                                },
                                'formula':{
                                       'metric_core % cycles in AVX-512 license':'100*c/(a+b+c)'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_SMI number of SMIs per sec':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'MSR_EVENT:msr=0x34:type=FREERUN:scope=PACKAGE'
                                },
                                'formula':{
                                       'metric_SMI number of SMIs per sec':'a+0'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_core initiated local dram read bandwidth (MB/sec)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'OFFCORE_RESPONSE:request=ALL_READS:response=L3_MISS.ANY_SNOOP:ocr_msr_val=0x3f840007f7'
                                },
                                'formula':{
                                       'metric_core initiated local dram read bandwidth (MB/sec)':'a*64/1000000'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_core initiated remote dram read bandwidth (MB/sec)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'OFFCORE_RESPONSE:request=ALL_READS:response=L3_MISS.ANY_SNOOP:ocr_msr_val=0x3fB80007f7'
                                },
                                'formula':{
                                       'metric_core initiated remote dram read bandwidth (MB/sec)':'a*64/1000000'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_core initiated local DCPMEM read bandwidth (MB/sec)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'OFFCORE_RESPONSE:request=ALL_READS:response=L3_MISS.ANY_SNOOP:ocr_msr_val=0x3f804007f7'
                                },
                                'formula':{
                                       'metric_core initiated local DCPMEM read bandwidth (MB/sec)':'a*64/1000000'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_core initiated remote DCPMEM read bandwidth (MB/sec)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'OFFCORE_RESPONSE:request=ALL_READS:response=L3_MISS.ANY_SNOOP:ocr_msr_val=0x3f838007f7'
                                },
                                'formula':{
                                       'metric_core initiated remote DCPMEM read bandwidth (MB/sec)':'a*64/1000000'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_M3UPI avg all VN0 entries':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_M3UPI_CLOCKTICKS','c':'UNC_M3UPI_RxC_OCCUPANCY_VN0.BL_RSP:u0x7f'
                                },
                                'formula':{
                                       'metric_M3UPI avg all VN0 entries':'c/a'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_M3UPI % cycles when all VN0 has 1 or more entries':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_M3UPI_CLOCKTICKS','c':'UNC_M3UPI_RxC_OCCUPANCY_VN0.BL_RSP:u0x7f:t=1'
                                },
                                'formula':{
                                       'metric_M3UPI % cycles when all VN0 has 1 or more entries':'100*c/a'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_M3UPI % cycles when all VN0 has 10 or more entries':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_M3UPI_CLOCKTICKS','c':'UNC_M3UPI_RxC_OCCUPANCY_VN0.BL_RSP:u0x7f:t=10'
                                },
                                'formula':{
                                       'metric_M3UPI % cycles when all VN0 has 10 or more entries':'100*c/a'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_M3UPI % cycles when all VN0 has 30 or more entries':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_M3UPI_CLOCKTICKS','c':'UNC_M3UPI_RxC_OCCUPANCY_VN0.BL_RSP:u0x7f:t=30'
                                },
                                'formula':{
                                       'metric_M3UPI % cycles when all VN0 has 30 or more entries':'100*c/a'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_M3UPI % cycles when all VN0 has 50 or more entries':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'UNC_M3UPI_CLOCKTICKS','c':'UNC_M3UPI_RxC_OCCUPANCY_VN0.BL_RSP:u0x7f:t=50'
                                },
                                'formula':{
                                       'metric_M3UPI % cycles when all VN0 has 50 or more entries':'100*c/a'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_TMAM_....ICache_Misses(%)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'ICACHE_16B.IFDATA_STALL','b':'ICACHE_16B.IFDATA_STALL:c1:e1','d':'CPU_CLK_UNHALTED.THREAD'
                                },
                                'formula':{
                                       'metric_TMAM_....ICache_Misses(%)':'100*(a+2*b)/d'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_TMAM_....ITLB_Misses(%)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'ICACHE_64B.IFTAG_STALL','d':'CPU_CLK_UNHALTED.THREAD'
                                },
                                'formula':{
                                       'metric_TMAM_....ITLB_Misses(%)':'100*a/d'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_TMAM_....Branch_Resteers(%)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'INT_MISC.CLEAR_RESTEER_CYCLES','b':'BACLEARS.ANY','j':'CPU_CLK_UNHALTED.THREAD'
                                },
                                'formula':{
                                       'metric_TMAM_....Branch_Resteers(%)':'100*(a+9*b)/j'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_TMAM_......Mispredicts_Resteers(%)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'INT_MISC.CLEAR_RESTEER_CYCLES','b':'MACHINE_CLEARS.COUNT','c':'BR_MISP_RETIRED.ALL_BRANCHES','j':'CPU_CLK_UNHALTED.THREAD'
                                },
                                'formula':{
                                       'metric_TMAM_......Mispredicts_Resteers(%)':'100*a*(c/(b+c))/j'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_TMAM_......Clears_Resteers(%)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'INT_MISC.CLEAR_RESTEER_CYCLES','b':'MACHINE_CLEARS.COUNT','c':'BR_MISP_RETIRED.ALL_BRANCHES','j':'CPU_CLK_UNHALTED.THREAD'
                                },
                                'formula':{
                                       'metric_TMAM_......Clears_Resteers(%)':'100*a*(1-(c/(b+c)))/j'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_TMAM_......Unknown_Branches_Resteers(%)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'b':'BACLEARS.ANY','j':'CPU_CLK_UNHALTED.THREAD'
                                },
                                'formula':{
                                       'metric_TMAM_......Unknown_Branches_Resteers(%)':'100*(9*b)/j'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_TMAM_....DSB_Switches(%)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'DSB2MITE_SWITCHES.PENALTY_CYCLES','d':'CPU_CLK_UNHALTED.THREAD'
                                },
                                'formula':{
                                       'metric_TMAM_....DSB_Switches(%)':'100*a/d'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_TMAM_....MS_Switches(%)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'IDQ.MS_SWITCHES','d':'CPU_CLK_UNHALTED.THREAD'
                                },
                                'formula':{
                                       'metric_TMAM_....MS_Switches(%)':'100*2*a/d'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_TMAM_....L1_Bound(%)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'CYCLE_ACTIVITY.STALLS_MEM_ANY','b':'CYCLE_ACTIVITY.STALLS_L1D_MISS','c':'CPU_CLK_UNHALTED.THREAD'
                                },
                                'formula':{
                                       'metric_TMAM_....L1_Bound(%)':'100*(a-b)/c'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_TMAM_......DTLB_Load(%)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'DTLB_LOAD_MISSES.STLB_HIT','b':'DTLB_LOAD_MISSES.WALK_ACTIVE','c':'CPU_CLK_UNHALTED.THREAD'
                                },
                                'formula':{
                                       'metric_TMAM_......DTLB_Load(%)':'100*(7*a+b)/c'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_TMAM_......Store_Fwd_Blk(%)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'LD_BLOCKS.STORE_FORWARD','b':'CPU_CLK_UNHALTED.THREAD'
                                },
                                'formula':{
                                       'metric_TMAM_......Store_Fwd_Blk(%)':'100*((13 * a) / b) '
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_TMAM_......Lock_Latency(%)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'MEM_INST_RETIRED.LOCK_LOADS','b':'CPU_CLK_UNHALTED.THREAD','c':'MEM_INST_RETIRED.ALL_STORES','d':'OFFCORE_REQUESTS_OUTSTANDING.CYCLES_WITH_DEMAND_RFO'
                                },
                                'formula':{
                                       'metric_TMAM_......Lock_Latency(%)':'100*(((a / c)* (case when b > d then d else b end))/ b) '
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_TMAM_....L2_Bound(%)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'CYCLE_ACTIVITY.STALLS_L1D_MISS','b':'CYCLE_ACTIVITY.STALLS_L2_MISS','c':'CPU_CLK_UNHALTED.THREAD'
                                },
                                'formula':{
                                       'metric_TMAM_....L2_Bound(%)':'100*(a-b)/c'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_TMAM_....L3_Bound(%)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'CYCLE_ACTIVITY.STALLS_L2_MISS','c':'CYCLE_ACTIVITY.STALLS_L3_MISS','d':'CPU_CLK_UNHALTED.THREAD'
                                },
                                'formula':{
                                       'metric_TMAM_....L3_Bound(%)':'100*(a-c)/d'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_TMAM_......Contested_Accesses(%)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'MEM_LOAD_L3_HIT_RETIRED.XSNP_HITM','b':'CPU_CLK_UNHALTED.THREAD','c':'MEM_LOAD_L3_HIT_RETIRED.XSNP_MISS'
                                },
                                'formula':{
                                       'metric_TMAM_......Contested_Accesses(%)':'100*(60 * ( a + c ) / b) '
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_TMAM_......Data_Sharing(%)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'MEM_LOAD_L3_HIT_RETIRED.XSNP_HIT','b':'CPU_CLK_UNHALTED.THREAD'
                                },
                                'formula':{
                                       'metric_TMAM_......Data_Sharing(%)':'100*(43 * a /b) '
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_TMAM_......L3_Latency(%)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'b':'OFFCORE_REQUESTS_OUTSTANDING.DEMAND_DATA_RD_GE_6','c':'OFFCORE_REQUESTS_OUTSTANDING.L3_MISS_DEMAND_DATA_RD_GE_6','d':'CPU_CLK_UNHALTED.THREAD','e':'OFFCORE_REQUESTS_OUTSTANDING.CYCLES_WITH_DEMAND_DATA_RD','f':'OFFCORE_REQUESTS_OUTSTANDING.CYCLES_WITH_L3_MISS_DEMAND_DATA_RD'
                                },
                                'formula':{
                                       'metric_TMAM_......L3_Latency(%)':'100*((((case when e > d then d else e end)-(case when f > d then d else f end))/d)-(((case when b > d then d else b end)-(case when d > d then d else c end))/d))'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_TMAM_......L3_Bandwidth(%)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'b':'OFFCORE_REQUESTS_OUTSTANDING.DEMAND_DATA_RD_GE_6','c':'OFFCORE_REQUESTS_OUTSTANDING.L3_MISS_DEMAND_DATA_RD_GE_6','d':'CPU_CLK_UNHALTED.THREAD'
                                },
                                'formula':{
                                       'metric_TMAM_......L3_Bandwidth(%)':'100*((case when b > d then d else b end)-(case when c > d then d else c end))/d'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_TMAM_....MEM_Bound(%)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'CYCLE_ACTIVITY.STALLS_L3_MISS','d':'CPU_CLK_UNHALTED.THREAD'
                                },
                                'formula':{
                                       'metric_TMAM_....MEM_Bound(%)':'100*(a/d)'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_TMAM_......MEM_Bandwidth(%)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'OFFCORE_REQUESTS_OUTSTANDING.L3_MISS_DEMAND_DATA_RD_GE_6','b':'CPU_CLK_UNHALTED.THREAD'
                                },
                                'formula':{
                                       'metric_TMAM_......MEM_Bandwidth(%)':'100*((case when a > b then b else a end))/b'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_TMAM_......MEM_Latency(%)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'OFFCORE_REQUESTS_OUTSTANDING.L3_MISS_DEMAND_DATA_RD_GE_6','b':'OFFCORE_REQUESTS_OUTSTANDING.CYCLES_WITH_L3_MISS_DEMAND_DATA_RD','c':'CPU_CLK_UNHALTED.THREAD'
                                },
                                'formula':{
                                       'metric_TMAM_......MEM_Latency(%)':'100*(((case when b > c then c else b end))-((case when a > c then c else a end)))/c'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_TMAM_....Stores_Bound(%)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'EXE_ACTIVITY.BOUND_ON_STORES','b':'CPU_CLK_UNHALTED.THREAD'
                                },
                                'formula':{
                                       'metric_TMAM_....Stores_Bound(%)':'100*(a/b)'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_TMAM_....Divider(%)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'ARITH.DIVIDER_ACTIVE','b':'CPU_CLK_UNHALTED.THREAD'
                                },
                                'formula':{
                                       'metric_TMAM_....Divider(%)':'100*a/b'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_TMAM_....Ports_Utilization(%)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'd':'EXE_ACTIVITY.EXE_BOUND_0_PORTS','f':'EXE_ACTIVITY.1_PORTS_UTIL','g':'CYCLE_ACTIVITY.STALLS_MEM_ANY','h':'EXE_ACTIVITY.BOUND_ON_STORES','j':'EXE_ACTIVITY.2_PORTS_UTIL','p':'CPU_CLK_UNHALTED.THREAD','q':'INST_RETIRED.ANY'
                                },
                                'formula':{
                                       'metric_TMAM_....Ports_Utilization(%)':'100*((d+f+(case when ((q/p)>1.8) then j else 0 end)+g+h)-g-h)/p'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_TMAM_....FP_Arith(%)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'INST_RETIRED.ANY','b':'UOPS_EXECUTED.X87','c':'UOPS_RETIRED.RETIRE_SLOTS','d':'FP_ARITH_INST_RETIRED.SCALAR_SINGLE','e':'FP_ARITH_INST_RETIRED.SCALAR_DOUBLE','f':'FP_ARITH_INST_RETIRED.128B_PACKED_DOUBLE','g':'FP_ARITH_INST_RETIRED.128B_PACKED_SINGLE','h':'FP_ARITH_INST_RETIRED.256B_PACKED_SINGLE','j':'FP_ARITH_INST_RETIRED.256B_PACKED_DOUBLE','k':'UOPS_EXECUTED.THREAD','m':'FP_ARITH_INST_RETIRED.512B_PACKED_SINGLE','n':'FP_ARITH_INST_RETIRED.512B_PACKED_DOUBLE'
                                },
                                'formula':{
                                       'metric_TMAM_....FP_Arith(%)':'100*((b/k)+((d+e+f+g+h+j+m+n)/c))'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            },
             'metric_TMAM_....Other(%)':{
                                'sum_func':cores_sum,   
                                'events':{
                                      'a':'INST_RETIRED.ANY','b':'UOPS_EXECUTED.X87','c':'UOPS_RETIRED.RETIRE_SLOTS','d':'FP_ARITH_INST_RETIRED.SCALAR_SINGLE','e':'FP_ARITH_INST_RETIRED.SCALAR_DOUBLE','f':'FP_ARITH_INST_RETIRED.128B_PACKED_DOUBLE','g':'FP_ARITH_INST_RETIRED.128B_PACKED_SINGLE','h':'FP_ARITH_INST_RETIRED.256B_PACKED_SINGLE','j':'FP_ARITH_INST_RETIRED.256B_PACKED_DOUBLE','k':'UOPS_EXECUTED.THREAD','m':'FP_ARITH_INST_RETIRED.512B_PACKED_SINGLE','n':'FP_ARITH_INST_RETIRED.512B_PACKED_DOUBLE'
                                },
                                'formula':{
                                       'metric_TMAM_....Other(%)':'100*(1-((b/k)+((d+e+f+g+h+j+m+n)/c)))'
                                },
                                'fmt':lambda l: F.round(l, 3)
                            }
        })
        self.emon_df=None
        self.effective_metric=None
        self.sc = sc
        
    def list_metric(self):
        if self.effective_metric is None:
            self.get_effective_metric()
        for k in self.effective_metric:
            m=self.emon_metrics[k]
            print(k)
            for fk,fm in m['formula'].items():
                print("    ",fk)
            
    def load_emon(self):
        emondata=self.sc.textFile(self.emon_file)
        emondf=emondata.mapPartitions(splits).toDF()
        emondf=emondf.withColumn("id", F.monotonically_increasing_id())
        iddf=emondf.where(emondf._1.rlike("\d\d/")).selectExpr("_1 as r_1","_2 as r_2","id as r_id")
        jfid=emondf.where(emondf._1.rlike("[A-Z]{3}")).join(iddf,on=[emondf.id>iddf.r_id]).groupBy('id').agg(F.max('r_id').alias('r_id'))
        iddf=iddf.join(jfid,on='r_id',how='left')
        emondf=emondf.where(emondf._1.rlike("[A-Z]{3}")).join(iddf,on='id',how='left')
        emondf.cache()
        self.emon_df=emondf

    def get_effective_metric(self):
        if self.emon_df==None:
            self.load_emon()

        emondf=self.emon_df
        effective_metric=[]

        progress = IntProgress(layout=Layout(width='80%', height='40px'))
        progress.max = len(self.emon_metrics)
        progress.description = 'Calculate Effective Metrics'
        display(progress)
        progress.value=0

        for k,m in self.emon_metrics.items():
            join_df=None
            progress.value=progress.value+1
            for alias,event in m['events'].items():
                if join_df is None:
                    join_df=emondf.where("_1='{:s}'".format(event)).select('r_id')
                else:
                    join_df=join_df.join(emondf.where("_1='{:s}'".format(event)).select('r_id'),on='r_id',how='left')
            if join_df.count()>0:
                effective_metric.append(k)
        progress.value=progress.value+1
        self.effective_metric=effective_metric
        
    def generate_emon_trace_view(self,id=0,collected_cores=list(range(14*4))):
        if self.emon_df==None:
            self.load_emon()
        if self.effective_metric is None:
            self.get_effective_metric()
            
        emondf=self.emon_df
        trace_events=[]
        for k in self.effective_metric:
            m=self.emon_metrics[k]
            join_df=None
            for alias,event in m['events'].items():
                if join_df is None:
                    join_df=emondf.where("_1='{:s}'".format(event)).select('r_1','r_2','_1','_2','r_id',(m['sum_func'](collected_cores)).alias(alias))
                else:
                    join_df=join_df.join(emondf.where("_1='{:s}'".format(event)).select('_1','_2','r_id',(m['sum_func'](collected_cores)).alias(alias)),on='r_id',how='left')
            rstdf=join_df.select(
                            F.lit(0).alias('tid'),
                            F.lit(id).alias('pid'),
                            F.lit('C').alias('ph'),
                            F.lit(k).alias('name'),
                            (F.unix_timestamp(F.concat_ws(' ','r_1','r_2'),'MM/dd/yyyy HH:mm:ss')*F.lit(1000)-F.lit(self.start_time)).alias("ts"),
                            F.struct(*[m['fmt'](F.expr(formula)).alias(col_name) for col_name,formula in m['formula'].items() ]).alias('args')
            ).where(F.col("ts").isNotNull()).orderBy('ts')
            trace_events.extend(rstdf.toJSON().collect())
        return trace_events
    
    def generate_trace_view(self,trace_output,collected_cores=list(range(14*4))):
        traces=[]
        traces.extend(self.generate_emon_trace_view(0,collected_cores))
        
        output='''
        {
            "traceEvents": [
        
        ''' + \
        ",\n".join(traces)\
       + '''
            ]
        }'''

        with open('/home/yuzhou/catapult-master/tracing/test_data/'+trace_output+'.json', 'w') as outfile: 
            outfile.write(output)

        print("http://sr507:1088/tracing_examples/trace_viewer.html#/tracing/test_data/"+trace_output+".json")

    def show_emon_metric(self,metric,sub_metric,core,draw=True,metric_define=None):
        if self.emon_df==None:
            self.load_emon()
        emondf=self.emon_df
        
        if metric is None or metric=='':
            for k in self.effective_metric:
                m=self.emon_metrics[k]
                if sub_metric in m['formula']:
                    break
            else:
                print("can't find metric",sub_metric)
                return        
        else:
            k=metric
        if metric_define is None:
            m= self.emon_metrics[k]
        else:
            m= metric_define[k]

        if type(core)==int:
            core=[core,]
        selectcol=count_sum(core)
            
        join_df=None
        for alias,event in m['events'].items():
            if join_df is None:
                join_df=emondf.where("_1='{:s}'".format(event)).select('r_1','r_2','_1','_2','r_id',selectcol.alias(alias))
            else:
                join_df=join_df.join(emondf.where("_1='{:s}'".format(event)).select('_1','_2','r_id',selectcol.alias(alias)),on='r_id',how='left')
        rstdf=join_df.select(
                     (F.unix_timestamp(F.concat_ws(' ','r_1','r_2'),'MM/dd/yyyy HH:mm:ss')*F.lit(1000)-F.lit(self.start_time)).alias("ts"),
                    m['fmt'](F.expr(m['formula'][sub_metric])).alias(sub_metric),
                    'r_id'
        ).where(F.col("ts").isNotNull()).orderBy('ts')
        
        metric_sum=rstdf.select(sub_metric).summary().toPandas()
        display(metric_sum)
        
        if draw:
            pddf=rstdf.toPandas()
            pddf['ts']=(pddf['ts']-pddf.loc[0,'ts'])/1000
            fig, axs = plt.subplots(nrows=1, ncols=2, sharey=True,figsize=(30,8),gridspec_kw = {'width_ratios':[1, 5]})
            plt.subplots_adjust(wspace=0.01)
            sns.violinplot(y=sub_metric, data=pddf, ax=axs[0],palette=['g'])
            axs[0].yaxis.grid(True, which='major')
            ax=axs[1]
            ax.stackplot(pddf['ts'], pddf[sub_metric],colors=['bisque'])
            #ymin, ymax = ax.get_ylim()
            ax2 = ax.twinx()
            ax2.set_ylim(ax.get_ylim())
            ax2.axhline(y=float(metric_sum.loc[4,sub_metric]), linewidth=2, color='r')
            ax2.axhline(y=float(metric_sum.loc[5,sub_metric]), linewidth=2, color='r')
            ax2.axhline(y=float(metric_sum.loc[6,sub_metric]), linewidth=2, color='r')
            ax2.axhline(y=float(metric_sum.loc[7,sub_metric]), linewidth=2, color='r')
            ax.set_xlabel('time (s)')
            ax.yaxis.grid(True, which='major')
            plt.show()
            
            hist_elapsedtime=rstdf.select('`{:s}`'.format(sub_metric)).rdd.flatMap(lambda x: x).histogram(15)
            fig, axs = plt.subplots(figsize=(30, 5))
            ax=axs
            binSides, binCounts = hist_elapsedtime
            binSides=[builtins.round(l,2) for l in binSides]

            N = len(binCounts)
            ind = numpy.arange(N)
            width = 0.5

            rects1 = ax.bar(ind+0.5, binCounts, width, color='b')

            ax.set_ylabel('Frequencies')
            ax.set_title(sub_metric)
            ax.set_xticks(numpy.arange(N+1))
            ax.set_xticklabels(binSides)
        return rstdf
        

    def get_reduce_metric(self,metric,core,sub_metric,agg_func):
        if self.emon_df==None:
            self.load_emon()
        emondf=self.emon_df
        k=metric
        m= self.emon_metrics[k]

        if type(core)==int:
            core=[core,]
        selectcol=count_sum(core)

        join_df=None
        for alias,event in m['events'].items():
            if join_df is None:
                join_df=emondf.where("_1='{:s}'".format(event)).select('r_1','r_2','_1','_2','r_id',selectcol.alias(alias))
            else:
                join_df=join_df.join(emondf.where("_1='{:s}'".format(event)).select('_1','_2','r_id',selectcol.alias(alias)),on='r_id',how='left')
        rstdf=join_df.select(
                     (F.unix_timestamp(F.concat_ws(' ','r_1','r_2'),'MM/dd/yyyy HH:mm:ss')*F.lit(1000)-F.lit(self.start_time)).alias("ts"),
                    m['fmt'](F.expr(m['formula'][sub_metric])).alias(sub_metric)
        ).where(F.col("ts").isNotNull())
        return rstdf.agg(*[l("`{:s}`".format(sub_metric)).alias(get_alias_name(sub_metric,l)) for l in agg_func]).toPandas()
   

    def get_reduce_metrics(self,core,agg_func=[F.max,F.mean,F.min]):
        coldf=None
        if self.effective_metric is None:
            self.get_effective_metric()

        progress = IntProgress(layout=Layout(width='80%', height='40px'))
        progress.max = len(self.effective_metric)
        progress.description = 'Calculate Effective Metrics'
        display(progress)
        progress.value=0
        
        for k in self.effective_metric:
            progress.value=progress.value+1
            m=self.emon_metrics[k]
            for fk,fm in m['formula'].items():
                df=self.get_reduce_metric(k,list(range(0,16)),fk,agg_func)
                df.columns=[f.__name__ for f in agg_func]
                df.index=[fk]
                if coldf is None:
                    coldf=df
                else:
                    coldf=coldf.append(df)
        progress.value=progress.value+1
        return coldf
        
        
class Application_Run:
    def __init__(self, appid,jobids,clients, sc, spark):
        self.appid=appid
        self.sc=sc
        
        self.app_log=App_Log_Analysis(appid,jobids, spark)
        self.clients=clients
        dfidx=0
        self.sardf={}
        self.emondf={}
        for dfidx,l in enumerate(clients):
            self.sardf[dfidx]=self.load_sar(l)
            self.emondf[dfidx]=self.load_emon(l)
#         self.emondf[0]=self.load_emon('sr595')
        self.starttime=0
        
#         tree = ET.parse("metrics.xml")
#         lst = tree.findall('metric')
#         self.metrics_list=[]
#         for item in lst:
#             if len(item.findall('constant'))==0:
#                 out='''             '{:s}':{{
#                                 'sum_func':cores_sum,   
#                                 'events':{{
#                                       {:s}
#                                 }},
#                                 'formula':{{
#                                        '{:s}':'{:s}'
#                                 }},
#                                 'fmt':lambda l: F.round(l, 3)
#                                             }}'''.format(item.get('name'),','.join(["'{:s}':'{:s}'".format(l.get('alias'),l.text) for l in item.findall('event')]),item.get('name'),item.findtext('formula'))
#             self.metrics_list.append(out)
                    
    def load_sar(self,slaver):
        sardata=self.sc.textFile("/tmp/sparkSarLog/"+self.appid+"."+slaver+".sar")
        sardf=sardata.mapPartitions(splits).toDF()
        sardf=sardf.where("_1<>'Average:'")
        sardf.cache()
        return sardf
        
    def load_emon(self,slaver):
        return Emon_Analysis("/tmp/sparkSarLog/"+self.appid+"."+slaver+".rst", self.sc)
    
    def generate_log_trace_view(self,showcpu=False):
        logevt = self.app_log.generate_log_trace_view(showcpu)
        #use event log's starttime
        self.starttime=self.app_log.starttime
        return logevt
    
    def col_df(self,cond,colname,args,slaver_id):
        sardf=self.sardf[slaver_id]
        starttime=self.starttime
        cpudf=sardf.where(cond)
        #cpudf.select(F.date_format(F.from_unixtime(F.lit(starttime/1000)), 'yyyy-MM-dd HH:mm:ss').alias('starttime'),'_1').show(1)

        cpudf=cpudf.withColumn('_1',F.unix_timestamp(F.concat_ws(' ',F.date_format(F.from_unixtime(F.lit(starttime/1000)), 'yyyy-MM-dd'),cpudf._1,cpudf._2),'yyyy-MM-dd hh:mm:ss a'))
        cpudf=cpudf.groupBy('_1').agg(
            F.sum(F.when(cpudf._3.rlike('^\d+$'),cpudf._3.astype(IntegerType())).otherwise(0)).alias('_3'),
            F.sum(cpudf._4).alias('_4'),
            F.sum(cpudf._5).alias('_5'),
            F.sum(cpudf._6).alias('_6'),
            F.sum(cpudf._7).alias('_7'),
            F.sum(cpudf._8).alias('_8'),
            F.sum(cpudf._9).alias('_9'),
            F.sum(cpudf._10).alias('_10'),
            F.sum(cpudf._11).alias('_11'),
        )

        traces=cpudf.orderBy(cpudf._1).select(
                F.lit(0).alias('tid'),
                (F.expr("_1*1000")-F.lit(starttime)).alias('ts'),
                F.lit(slaver_id).alias('pid'),
                F.lit('C').alias('ph'),
                F.lit(colname).alias('name'),
                args(cpudf).alias('args')
            ).toJSON().collect()
        return traces

    def generate_sar_trace_view(self,slaver,id):
        trace_events=[]
        trace_events.extend(self.col_df("_3='all'",             "all cpu%",    lambda l: F.struct(F.floor(l['_4'].astype(FloatType())).alias('user'),F.floor(l['_6'].astype(FloatType())).alias('system')),                            id))
        trace_events.extend(self.col_df("_3 like 'dev8%'",      "disk ",       lambda l: F.struct(F.floor(F.expr('cast(_5 as float)*512/1024/1024')).alias('read'),F.floor(F.expr('cast(_6 as float) *512/1024/1024')).alias('write')),id))
        trace_events.extend(self.col_df("_3 like 'eth%'",       "eth ",        lambda l: F.struct(F.floor(F.expr('cast(_6 as float)/1024')).alias('rxmb/s'),F.floor(F.expr('cast(_7 as float)/1024')).alias('txmb/s')),                id))
        trace_events.extend(self.col_df("_3 = 'lo'",            "lo ",         lambda l: F.struct(F.floor(F.expr('cast (_6 as float)/1024')).alias('rxmb/s'),F.floor(F.expr('cast (_7 as float)/1024')).alias('txmb/s')),              id))
        trace_events.extend(self.col_df(self.sardf[id]._3.rlike('^\d+$'),"mem % ",      lambda l: F.struct(F.floor(l._7*F.lit(100)/(l._3+l._4)).alias('cached'),
                                                                                                       F.floor(l._6*F.lit(100)/(l._3+l._4)).alias('buffered'),
                                                                                                       F.floor(l._5-(l._6+l._7)*F.lit(100)/(l._3+l._4)).alias('all')),                                                                 id))
        trace_events.extend(self.col_df(self.sardf[id]._3.rlike('^\d+$'),"mem cmt % ",  lambda l: F.struct(F.floor(l._8*F.lit(100)/(l._3+l._4)).alias('commit/phy'),
                                                                                                           F.floor(l._9-l._8*F.lit(100)/(l._3+l._4)).alias('commit/all')),                                                             id))
        trace_events.append(json.dumps({"name": "process_name","ph": "M","pid":id,"tid":0,"args":{"name":"fsys."+slaver}}))
        return trace_events

    def generate_emon_trace_view(self,id,collected_cores):
        emondf=self.emondf[id]
        emondf.start_time=self.starttime
        return emondf.generate_emon_trace_view(id,collected_cores)
    
    def generate_trace_view(self,showcpu=False,showsar=True,showemon=True,collected_cores=list(range(14*4))):
        traces=[]
        traces.extend(self.generate_log_trace_view(showcpu))
        if showsar:
            for idx,l in enumerate(self.clients):
                traces.extend(self.generate_sar_trace_view(l,idx))
        if showemon:
            traces.extend(self.generate_emon_trace_view(0,collected_cores))
        
        output='''
        {
            "traceEvents": [
        
        ''' + \
        ",\n".join(traces)\
       + '''
            ]
        }'''

        with open('/home/yuzhou/catapult-master/tracing/test_data/'+self.appid+'.json', 'w') as outfile:  
            outfile.write(output)

        print("http://sr525:1088/tracing_examples/trace_viewer.html#/tracing/test_data/"+self.appid+".json")
        
        
 

        