# Kudu Block Cache on Intel Optane PMem User Guide

## Prerequisites

Before getting started with kudu block cache on Optane PMem, you should have set up a working 
kudu environment and your machine should have Optane PMem setup.

Please refer to documentation at
["Installing Apache Kudu"](https://kudu.apache.org/docs/installation.html)
for detailed guidance on building apache kudu

Please refer to documentation at 
["Quick Start Guide: Provision Intel® Optane™ DC Persistent Memory"](https://software.intel.com/en-us/articles/quick-start-guide-configure-intel-optane-dc-persistent-memory-on-linux)
for detailed to setup Optane PMem with App Direct Mode.

Currently kudu block cache on Optane PMem does not support multiple path, So please refer to documentation at
["Creating dm-striped Devices"](https://pmem.io/2018/05/15/using_persistent_memory_devices_with_the_linux_device_mapper.html)
to bind pmem devices as one.

## Configuration for kudu tablet server 

To enable block cache on Optane PMem, you need add the following configuration
```
--block_cache_type=NVM
--block_cache_capacity_mb=${SIZE_IN_MB}
--nvm_cache_path=${PMEM_PATH}
```
Please refer to documentation at 
["Apache Kudu Configuration Reference"](https://kudu.apache.org/docs/configuration_reference.html)
for detailed the descriptions about kudu configuration

## Run YCSB benchmark

You can use YCSB to benchmark kudu block cache. Please get YCSB from 
<https://github.com/brianfrankcooper/YCSB/>

You need load data into kudu first
```
${YCSB_HOME}/bin/ycsb load kudu -threads ${THREAD_NUMBER} -P ${WORKDLOAD}
```
Then you can run ycsb to evaluate the performance
```
${YCSB_HOME}/bin/ycsb run kudu -threads ${THREAD_NUMBER} -P ${WORKDLOAD}
```
Please refer to recommended configuration from the blog
["Maximizing performance of Apache Kudu block cache with Intel Optane DCPMM"](https://blog.cloudera.com/maximizing-performance-of-apache-kudu-block-cache-with-intel-optane-dcpmm)

## Limitations
For the scenario that data will exceed the block cache capacity.
Memkind 1.9.0 and kernel 4.18 is recommended to avoid the unexpected issue.
