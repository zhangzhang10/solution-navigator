## Configurare Hardware
Directories exposing Optane PMem hardware on each socket. For example, on a two socket system the mounted DCPMM directories should appear as /mnt/pmem0 and /mnt/pmem1. Correctly installed Optane PMem must be formatted and mounted on every cluster worker node.
   
      // use impctl command to show topology and dimm info of DCPM
      impctl show -topology
      impctl show -dimm
      // provision dcpm in app direct mode
      ipmctl create -goal PersistentMemoryType=AppDirect
      // reboot system to make configuration take affect
      reboot
      // check capacity provisioned for app direct mode(AppDirectCapacity)
      impctl show -memoryresources
      // show the DCPM region information
      impctl show -region
      // create namespace based on the region, multi namespaces can be created on a single region
      ndctl create-namespace -m fsdax -r region0
      ndctl create-namespace -m fsdax -r region1
      // format pmem*
      mkfs.ext4 /dev/pmem0  
      mkfs.ext4 /dev/pmem1 
      // show the created namespaces
      fdisk -l
      // create and mount file system
      mount -o dax /dev/pmem0 /mnt/pmem0
      mount -o dax /dev/pmem1 /mnt/pmem1

In this case file systems are generated for 2 numa nodes, which can be checked by "numactl --hardware". For a different number of numa nodes, a corresponding number of namespaces should be created to assure correct file system paths mapping to numa nodes.

Please valiate Optane PMem mount in DAX mode like below. You should find **DAX** listed in mode list for each PMEM entry like below:

      #mount | grep dax 
      /dev/pmem0 on /mnt/pmem0 type ext4 (rw,relatime,dax)
      /dev/pmem1 on /mnt/pmem1 type ext4 (rw,relatime,dax) 

## Install Memkind library
Memkind library is required to be installed on every cluster worker node. Please use the latest Memkind version. Compile Memkind based on your system.

Please download Memkind from the [official website](https://github.com/memkind/memkind). Please install required dependencies list in [Memkind website](https://github.com/memkind/memkind#dependencies) and build it from source following the [instructions](https://github.com/memkind/memkind#building-and-installing)

Please copy the built so under the folder.

      sudo cp -r /usr/local/lib/* /lib64 

Please validate your Memkind is installed properly by checking memkind is under lib64 folder.

      #ls /lib64 | grep memkind*
      libmemkind.so 

# Setup Hadoop
Please set up Hadoop following [the official website](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html#Installing_Software)

Please validate you installation by running [Hadoop benchmark](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/Benchmarking.html) successfully.

# Install Spark
Please refer to the supported Spark version in [Link] (https://github.com/Intel-bigdata/Solution_navigator/blob/master/storage_mem_extension/storage-memory-extension-user-guide.md#prerequisites)

# Update Spark runtime configurations
Build Spark from source to enable numa-binding support

       https://github.com/Intel-bigdata/OAP/blob/branch-0.7-spark-2.4.x/docs/Developer-Guide.md#enable-numa-binding-for-dcpmm-in-spark

# Setup K-means Workload

Please download HiBench from [HiBench webpage](https://github.com/intel-hadoop/HiBench.git).

Modify pom.xml under sparkbench, sparkbench/streaming and sparkbench/structuredStreaming/ folders to add support of spark2.4.

Update files to include configuration of StorageLevel and initMode
in HiBench-master\sparkbench\ml\src\main\scala\com\intel\sparkbench\ml\DenseKMeans.scala file do follow modifications:

    import org.apache.spark.storage.StorageLevel
    case class Params(
          input: String = null,
          k: Int = -1,
          numIterations: Int = 10,
          initializationMode: InitializationMode = Parallel,
          storageLevel: Int = 3)
    opt[Int]("storageLevel")
            .required()
            .text(s"storage level, required")
            .action((x, c) => c.copy(storageLevel = x))

in HiBench-master\bin\functions\hibench_prop_env_mapping.py file add following part:

    K_INIT_MODE="hibench.kmeans.initMode",
    K_STORAGE_LEVEL="hibench.kmeans.storageLevel" 

in HiBench-master\bin\workloads\ml\kmeans\spark\run.sh file add parameters like bellow:

    run_spark_job com.intel.hibench.sparkbench.ml.DenseKMeans -k $K --numIterations $MAX_ITERATION --storageLevel $K_STORAGE_LEVEL --initMode $K_INIT_MODE $INPUT_HDFS/samples
 
Compile Highbench.

Configure ${HiBench_Home}/conf/spark.conf, set the hibench.spark.home to the spark you installed.

Configure ${HiBench_Home}/conf/workloads/ml, the num_of_samples correspond to different size of data set, set to generate relative size of data.
 
Specify the dateset in ${HiBench_Home}/conf/hibench.conf
 
Run ${HiBench_Home}/bin/workloads/ml/kmeans/prepare/prepare.sh to generate data

## Running Spark Kmeans
See [https://github.com/Intel-bigdata/HiBench/blob/master/docs/run-sparkbench.md] 

Configure HiBench with Spark PMem storage level.
