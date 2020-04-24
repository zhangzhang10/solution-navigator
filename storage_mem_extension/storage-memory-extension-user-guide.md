# Storage extension with Optane PMem Userguide

## Prerequisites

Before getting start with storage extension with Optane PMem, your machine should have Intel Optane PMem setup and you should have memkind being installed. For memkind installation, please refer [memkind webpage](https://github.com/memkmemkindind/).

Please refer to documentation at ["Quick Start Guide: Provision Intel® Optane DC Persistent Memory"](https://software.intel.com/en-us/articles/quick-start-guide-configure-intel-optane-dc-persistent-memory-on-linux) for detailed to setup Optane PMem with App Direct Mode.

## Configuration

To enable rdd cache on Intel Optane PMem, you need add the following configurations:
```
spark.memory.pmem.initial.path [Your Optane PMem paths seperate with comma]
spark.memory.pmem.initial.size [Your Optane PMem size in GB]
spark.yarn.numa.enabled true
spark.yarn.numa.num [Your numa node number]
```

## Use Optane PMem to cache data

There's a new StorageLevel: PMEM_AND_DISK being added to cache data to Optane PMem, at the places you previously cache/persist data to memory, use PMEM_AND_DISK to substitute the previous StorageLevel, data will be cached to Optane PMem.
```
persist(StorageLevel.PMEM_AND_DISK)
```

## Run K-means benchmark

You can use [Hibench](https://github.com/Intel-bigdata/HiBench) to run K-means workload:

## Limitations

For the scenario that data will exceed the block cache capacity. Memkind 1.9.0 and kernel 4.18 is recommended to avoid the unexpected issue.
