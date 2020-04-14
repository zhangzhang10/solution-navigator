# Storage extension with DCPM Userguide

## Prerequisites

Before getting start with storage extension with DCPM, your machine should have Intel DCPM setup and you should have memkind being installed. For memkind installation, please refer [memkind webpage](https://github.com/memkmemkindind/).

Please refer to documentation at ["Quick Start Guide: Provision Intel® Optane DC Persistent Memory"](https://software.intel.com/en-us/articles/quick-start-guide-configure-intel-optane-dc-persistent-memory-on-linux) for detailed to setup DCPM with App Direct Mode.

## Configuration

To enable block cache on Intel DCPM, you need add the following configurations:

    spark.memory.aep.initial.path xxx,xxx (list DCPM paths seperate with comma)
    spark.memory.aep.initial.size xxxG (set the initial size of DCPM)

## Use DCPM to cache data

There's a new StorageLevel: AEP being added to cache data to DCPM, at the places you previously cache/persist data to memory, use AEP substitute the previous StorageLevel, data will be cached to DCPM.

    persist(StorageLevel.AEP)

## Run K-means benchmark

You can use [Hibench](https://github.com/Intel-bigdata/HiBench) to run K-means workload:

    update the places to cache data to AEP

Run K-means workload with:

    ${HiBench_Home}/bin/workloads/ml/kmeans/spark/run.sh

## Limitations

For the scenario that data will exceed the block cache capacity. Memkind 1.9.0 and kernel 4.18 is recommended to avoid the unexpected issue.