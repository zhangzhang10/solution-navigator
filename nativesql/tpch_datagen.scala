import com.databricks.spark.sql.perf.tpch._


val scaleFactor = "10" // scaleFactor defines the size of the dataset to generate (in GB).
val numPartitions = 20  // how many dsdgen partitions to run - number of input tasks.

val databaseName = s"date_tpch_$scaleFactor" // name of database to create.
val rootDir = s"$databaseName" // root directory of location to create data in.
val format = "parquet" // valid spark format like parquet "parquet".

val tables = new TPCHTables(spark.sqlContext,
    dbgenDir = "/root/git/tpch-dbgen", // location of dsdgen
    scaleFactor = scaleFactor,
    useDoubleForDecimal = true, // true to replace DecimalType with DoubleType
    useStringForDate = false) // true to replace DateType with StringType

spark.sqlContext.setConf("spark.sql.files.maxRecordsPerFile", "20000000")

tables.genData(
    location = rootDir,
    format = format,
    overwrite = true, // overwrite the data that is already there
    partitionTables = false, // create the partitioned fact tables
    clusterByPartitionColumns = false, // shuffle to get partitions coalesced into single files.
    filterOutNullPartitionValues = false, // true to filter out the partition with NULL key value
    tableFilter = "", // "" means generate all tables
    numPartitions = numPartitions) // how many dsdgen partitions to run - number of input tasks.

// Create the specified database
sql(s"drop database $databaseName CASCADE")
sql(s"create database $databaseName")
// Create metastore tables in a specified database for your data.
// Once tables are created, the current database will be switched to the specified database.
tables.createExternalTables(rootDir, "parquet", databaseName, overwrite = true, discoverPartitions = false)
