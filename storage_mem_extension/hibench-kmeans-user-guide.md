# Use Optane PMem in HiBench Kmeans workload 
## Generate raw dataset
Download HiBench from [HiBench webpage](https://github.com/intel-hadoop/HiBench.git).

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
Specify configurations on how to run Kmeans workload.

modify ${HiBench_Home}/conf/hadoop.conf to set Hadoop to the correct directory

modify ${HiBench_Home}/conf/spark.conf to specify Spark you are using and configurations:
 
You must set spark.memory.aep.initial.path to the place where Optane PMem is mounted.

If you want to enable numa binding, please set spark.yarn.numa.enabled to true, to disable numa binding, set it to false.

modify kmeans workload to cache RDD to different storage:

update ${HiBench_Home}/sparkbench/ml/src/main/scala/com/intel/sparkbench/ml/DenseKMeans.scala to change like bellow:

    cache() => persist(StorageLevel.AEP)
 
recompile HiBench to involve these changes. 

Now you can choose to cache RDD to which storage level in ${HiBench_Home}/conf/workloads/ml/kmeans.conf.
 
And you can also change the init methodology to be random or parallel.
 
Run ${HiBench_Home}/bin/workloads/ml/kmeans/spark/run.sh to start spark kmeans workload
