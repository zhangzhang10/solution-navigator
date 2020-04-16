# Storage extension with Optane PMem Userguide

## Prerequisites

Before getting start with storage extension with Optane PMem, your machine should have Intel Optane PMem setup and you should have memkind being installed. For memkind installation, please refer [memkind webpage](https://github.com/memkmemkindind/).

Please refer to documentation at ["Quick Start Guide: Provision Intel® Optane DC Persistent Memory"](https://software.intel.com/en-us/articles/quick-start-guide-configure-intel-optane-dc-persistent-memory-on-linux) for detailed to setup Optane PMem with App Direct Mode.

## Configuration

To enable block cache on Intel Optane PMem, you need add the following configurations:

    spark.memory.aep.initial.path [Your Optane PMem paths seperate with comma]
    spark.memory.aep.initial.size [Your Optane PMem size in GB]

## Use Optane PMem to cache data

There's a new StorageLevel: AEP being added to cache data to Optane PMem, at the places you previously cache/persist data to memory, use AEP substitute the previous StorageLevel, data will be cached to Optane PMem.

    persist(StorageLevel.AEP)

## Example of Run K-means benchmark

You can use [Hibench](https://github.com/Intel-bigdata/HiBench) to run K-means workload:

First need to update source code to cache data to Optane PMem, modify ${HiBench_Home}/sparkbench/ml/src/main/scala/com/intel/sparkbench/ml/DenseKMeans.scala where data being cached to memory: 

    cache() => persist(StorageLevel.AEP)

Run K-means workload with:

    ${HiBench_Home}/bin/workloads/ml/kmeans/spark/run.sh

## Limitations

For the scenario that data will exceed the block cache capacity. Memkind 1.9.0 and kernel 4.18 is recommended to avoid the unexpected issue.