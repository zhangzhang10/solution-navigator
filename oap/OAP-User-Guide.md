# OAP Cache User Guide

## Prerequisites
Before configuring in Spark to use Optane PMem cache, you need to make sure the following:

Optane PMem hardwares are installed, formatted and mounted correctly on every cluster worker node with AppDirect mode(refer [guide](https://software.intel.com/en-us/articles/quick-start-guide-configure-intel-optane-dc-persistent-memory-on-linux)). You will get a mounted directory to use if you have done this. Usually, the Optane PMem on each socket will be mounted as a directory. For example, on a two sockets system, we may get two mounted directories named `/mnt/pmem0` and `/mnt/pmem1`.

Here is a quick guide to enable AppDirect mode for two socket system. Please run the below commands by superuser.
```
ipmctl create -goal PersistentMemoryType=AppDirect
reboot
ndctl create-namespace -m fsdax -r region0
ndctl create-namespace -m fsdax -r region1
mkfs.ext4 /dev/pmem0
mkfs.ext4 /dev/pmem1
mount -o dax /dev/pmem0 /mnt/pmem0
mount -o dax /dev/pmem1 /mnt/pmem1
```

Please validate Optane PMem mount in DAX mode like below. You should find **DAX** listed in mode list for each PMEM entry like below:
```
mount | grep dax 
```

The output should looks like as below:
```
/dev/pmem0 on /mnt/pmem0 type ext4 (rw,relatime,dax)
/dev/pmem1 on /mnt/pmem1 type ext4 (rw,relatime,dax) 
```

[Vmemcache](https://github.com/pmem/vmemcache) library has been installed on every cluster worker node if vmemcache strategy is chosen for DCPM cache. You can follow the build/install steps from vmemcache website and make sure libvmemcache.so exist in '/lib64' directory in each worker node. To build vmemcache lib from source, you can (for RPM-based linux as example):
```
     git clone https://github.com/pmem/vmemcache
     cd vmemcache
     mkdir build
     cd build
     cmake .. -DCMAKE_INSTALL_PREFIX=/usr -DCPACK_GENERATOR=rpm
     make package
     sudo rpm -i libvmemcache*.rpm
```

## Configure for NUMA

To achieve the optimum performance, we need to configure NUMA for binding executor to NUMA node and try access the right Optane PMem device on the same NUMA node. You need install numactl on each worker node. For example, on CentOS, run following command to install numactl.
```
yum install numactl -y
```

Check the numa nodes by command "numactl --hardware", typicallly it will have 2 numa nodes.
```
available: 2 nodes (0-1)
node 0 cpus: 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 48 49 50 51 52 53 54 55 56 57 58 59 60 61 62 63 64 65 66 67 68 69 70 71
node 0 size: 192129 MB
node 0 free: 92410 MB
node 1 cpus: 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 72 73 74 75 76 77 78 79 80 81 82 83 84 85 86 87 88 89 90 91 92 93 94 95
node 1 size: 193525 MB
node 1 free: 82864 MB
node distances:
node   0   1
  0:  10  21
  1:  21  10
```

## Setup Hadoop 2.7.5
Please set up Hadoop following [the official website](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html#Installing_Software) and make sure it works.

Make sure following points are correctly done:
1. Properly mount data disks and add them in hdfs-site.xml/yarn-site.xml.
```
hdfs-site.xml
   
   <property>
      <name>dfs.datanode.data.dir</name>
      <value>/mnt/DP_disk1/tpcds/dfs,/mnt/DP_disk2/tpcds/dfs</value>
   </property>

yarn-site.xml
   <property>
      <name>yarn.nodemanager.local-dirs</name>
            <value>/mnt/DP_disk1/tpcds/yarn,/mnt/DP_disk2/tpcds/yarn</value>
   </property>
```
2. disable pmem/vmem check in yarn-site.xml

```
   <property>
      <name>yarn.nodemanager.pmem-check-enabled</name>
      <value>false</value>
   </property>

   <property>
      <name>yarn.nodemanager.vmem-check-enabled</name>
      <value>false</value>
   </property>
```

Hadoop can be validated by running [Hadoop benchmark](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/Benchmarking.html).

## Install Spark 2.3.2

Please download Spark 2.3.2 from official [website](http://archive.apache.org/dist/spark/spark-2.3.2/).

## Install OAP for Spark 2.3.2

Build OAP fron source:
```
git clone https://github.com/Intel-bigdata/OAP
cd OAP
git checkout OPEN_SOURCE_VMEMCACHE_SPARK_2.3.2_PREVIEW
cd script
./apply_patch_to_spark.sh -v $SPAKR_VERSION
cd ..
mvn clean package -Pvmemcache,numa-binding -DskipTests
```
Copy target/oap-1.0.0-SNAPSHOT-with-spark-2.3.2.jar to master sever.

## Generate TPC-DS data

For Spark2.3.2, we use spark-sql-perf_2.11-0.4.11-SNAPSHOT.jar to do data generation and scripts are available for leverage.
1. Update Spark conf to include this jar file.
vi spark-defaults.conf(use full path to spark-sql-perf jar)
```
spark.files                        file:///home/spark-sql/extra-jars/spark-sql-perf_2.11-0.4.11-SNAPSHOT.jar
spark.executor.extraClassPath      ./spark-sql-perf_2.11-0.4.11-SNAPSHOT.jar
spark.driver.extraClassPath        file:///home/spark-sql/extra-jars/spark-sql-perf_2.11-0.4.11-SNAPSHOT.jar
```
2. update data_gen_*.scala to set correct dataset and data format

```
val scale = 3072  (data set: GB)
val format = "parquet"  (data format)
val codec = "snappy"   (data compression codec)
val namenode = "hostname"  (master's hostname)
```

3. update SPARK_HOME and executor settings in run_data_gen.sh, run ./run_data_gen.sh to start generate data.

## Enable OAP cache in Spark 2.3.2

For more details, you can refer this [doc](https://github.com/Intel-bigdata/OAP/blob/OPEN_SOURCE_VMEMCACHE_SPARK_2.3.2_PREVIEW/doc/DCPMM-Cache-Support-in-OAP.pdf)
1. modify spark-defaults.conf to include oap jar as extension
```
spark.sql.extensions              org.apache.spark.sql.OapExtensions
spark.files                        file:///home/spark-sql/extra-jars/oap-1.0.0-SNAPSHOT-with-spark-2.3.2.jar
spark.executor.extraClassPath      ./oap-1.0.0-SNAPSHOT-with-spark-2.3.2.jar
spark.driver.extraClassPath        file:///home/spark-sql/extra-jars/oap-1.0.0-SNAPSHOT-with-spark-2.3.2.jar

```
2. add OAP related parameters in spark-default.conf

For parquet,
```
spark.sql.oap.parquet.enable              true
spark.sql.oap.parquet.data.cache.enable   true
spark.sql.oap.fiberCache.memory.manager   self
spark.oap.cache.strategy                  vmem
spark.sql.oap.fiberCache.persistent.memory.initial.size     *g
spark.memory.offHeap.size                            *g
```
For ORC,
```
spark.sql.oap.orc.enable             true
spark.sql.orc.copyBatchToSpark       true
spark.sql.oap.orc.data.cache.enable  true
spark.oap.cache.strategy                  vmem
spark.sql.oap.fiberCache.persistent.memory.initial.size     *g
spark.memory.offHeap.size                            *g

```
3. enable numa-binding in spark-default.conf
```
spark.yarn.numa.enable    true
spark.yarn.numa.num       2
``` 
put persisten-memory.xml under $SPARK_HOME/conf.

4. update SPARK_HOME and master hostname in thriftserver script "upstream_spark_thrift_server_yarn_vmemcache.sh" and start thriftserver by run "./upstream_spark_thrift_server_yarn_vmemcache.sh start", verify 2 CoarseGrainedExecutorBackend started successfully on each node by command "jps".

```
7056 SecondaryNameNode
6513 DataNode
7827 NodeManager
50837 CoarseGrainedExecutorBackend
50037 SparkSubmit
6309 NameNode
7451 ResourceManager
50572 CoarseGrainedExecutorBackend
50444 ExecutorLauncher
73727 Jps

```

## Execute target queries

We have provided a python script for execution, update it based on your $SPARK_HOME, hostname and data size.

```
spark_home = 'full_path_to_spark-2.3.2'
beeline_args = '-u jdbc:hive2://hostname-master:10001 -n * -p *'
database_name = 'tpcds2048'
```
Then kickoff the execution by run './run_beeline.py'.

## OAP verification and trouble-shootings

1. To simply verify numa-binding takes affect, check 'numactl --cpubind=1 --membind=1' is included when start the executors.
2. If Vmemcache is properly initialized, the pmem disk usage will reach the initial cache size once the DCPMM cache is initialized and will not change during workload execution. You can check it by monitoring output of 'df -h'.
3. In Spark2.3.2, there is an issue which will cause spark sql performance drops and we have added the patch in OAP jar. You will need to verify this [patch](https://github.com/apache/spark/commit/5c67a9a7fa29836fc825504bbcc3c3fc820009c6) works properly from Spark UI.


