val wd = "spark-sql"
val scale = 3072
val p = 1.5
val catalog_returns_p = (263 * p).toInt
val catalog_sales_p = (2285 * p * 0.5 * 0.5).toInt
val store_returns_p = (429 * p).toInt
val store_sales_p = (3164 * p * 0.5 * 0.5).toInt
val web_returns_p = (198 * p).toInt
val web_sales_p = (1207 * p * 0.5 * 0.5).toInt
val format = "parquet"
val codec = "snappy"
val namenode = "sr416"
import com.databricks.spark.sql.perf.tpcds.Tables
spark.sqlContext.setConf(s"spark.sql.$format.compression.codec", codec)
val tables = new Tables(spark.sqlContext, s"/home/$wd/run_TPCDS/tools_forDataGen/tpcds-kit/tools", scale)
tables.genData(s"/home/$wd/work/tpcds$scale", format, true, false, true, false, false, "call_center", 1)
tables.genData(s"/home/$wd/work/tpcds$scale", format, true, false, true, false, false, "catalog_page", 1)
tables.genData(s"/home/$wd/work/tpcds$scale", format, true, false, true, false, false, "customer", 6)
tables.genData(s"/home/$wd/work/tpcds$scale", format, true, false, true, false, false, "customer_address", 1)
tables.genData(s"/home/$wd/work/tpcds$scale", format, true, false, true, false, false, "customer_demographics", 1)
tables.genData(s"/home/$wd/work/tpcds$scale", format, true, false, true, false, false, "date_dim", 1)
tables.genData(s"/home/$wd/work/tpcds$scale", format, true, false, true, false, false, "household_demographics", 1)
tables.genData(s"/home/$wd/work/tpcds$scale", format, true, false, true, false, false, "income_band", 1)
tables.genData(s"/home/$wd/work/tpcds$scale", format, true, false, true, false, false, "inventory", 6)
tables.genData(s"/home/$wd/work/tpcds$scale", format, true, false, true, false, false, "item", 1)
tables.genData(s"/home/$wd/work/tpcds$scale", format, true, false, true, false, false, "promotion", 1)
tables.genData(s"/home/$wd/work/tpcds$scale", format, true, false, true, false, false, "reason", 1)
tables.genData(s"/home/$wd/work/tpcds$scale", format, true, false, true, false, false, "ship_mode", 1)
tables.genData(s"/home/$wd/work/tpcds$scale", format, true, false, true, false, false, "store", 1)
tables.genData(s"/home/$wd/work/tpcds$scale", format, true, false, true, false, false, "time_dim", 1)
tables.genData(s"/home/$wd/work/tpcds$scale", format, true, false, true, false, false, "warehouse", 1)
tables.genData(s"/home/$wd/work/tpcds$scale", format, true, false, true, false, false, "web_page", 1)
tables.genData(s"/home/$wd/work/tpcds$scale", format, true, false, true, false, false, "web_site", 1)
tables.genData(s"/home/$wd/work/tpcds$scale", format, true, false, true, false, false, "catalog_sales", catalog_sales_p)
tables.genData(s"/home/$wd/work/tpcds$scale", format, true, false, true, false, false, "store_sales", store_sales_p)
tables.genData(s"/home/$wd/work/tpcds$scale", format, true, false, true, false, false, "web_sales", web_sales_p)
tables.genData(s"/home/$wd/work/tpcds$scale", format, true, false, true, false, false, "catalog_returns", catalog_returns_p)
tables.genData(s"/home/$wd/work/tpcds$scale", format, true, false, true, false, false, "store_returns", store_returns_p)
tables.genData(s"/home/$wd/work/tpcds$scale", format, true, false, true, false, false, "web_returns", web_returns_p)

spark.sql(s"create database tpcds$scale").show()
spark.sql(s"use tpcds$scale").show()
spark.catalog.createExternalTable("call_center", s"hdfs://${namenode}:9000/home/$wd/work/tpcds$scale/call_center", format)
spark.catalog.createExternalTable("catalog_page", s"hdfs://${namenode}:9000/home/$wd/work/tpcds$scale/catalog_page", format)
spark.catalog.createExternalTable("customer", s"hdfs://${namenode}:9000/home/$wd/work/tpcds$scale/customer", format)
spark.catalog.createExternalTable("customer_address", s"hdfs://${namenode}:9000/home/$wd/work/tpcds$scale/customer_address", format)
spark.catalog.createExternalTable("customer_demographics", s"hdfs://${namenode}:9000/home/$wd/work/tpcds$scale/customer_demographics", format)
spark.catalog.createExternalTable("date_dim", s"hdfs://${namenode}:9000/home/$wd/work/tpcds$scale/date_dim", format)
spark.catalog.createExternalTable("household_demographics", s"hdfs://${namenode}:9000/home/$wd/work/tpcds$scale/household_demographics", format)
spark.catalog.createExternalTable("income_band", s"hdfs://${namenode}:9000/home/$wd/work/tpcds$scale/income_band", format)
spark.catalog.createExternalTable("inventory", s"hdfs://${namenode}:9000/home/$wd/work/tpcds$scale/inventory", format)
spark.catalog.createExternalTable("item", s"hdfs://${namenode}:9000/home/$wd/work/tpcds$scale/item", format)
spark.catalog.createExternalTable("promotion", s"hdfs://${namenode}:9000/home/$wd/work/tpcds$scale/promotion", format)
spark.catalog.createExternalTable("reason", s"hdfs://${namenode}:9000/home/$wd/work/tpcds$scale/reason", format)
spark.catalog.createExternalTable("ship_mode", s"hdfs://${namenode}:9000/home/$wd/work/tpcds$scale/ship_mode", format)
spark.catalog.createExternalTable("store", s"hdfs://${namenode}:9000/home/$wd/work/tpcds$scale/store", format)
spark.catalog.createExternalTable("time_dim", s"hdfs://${namenode}:9000/home/$wd/work/tpcds$scale/time_dim", format)
spark.catalog.createExternalTable("warehouse", s"hdfs://${namenode}:9000/home/$wd/work/tpcds$scale/warehouse", format)
spark.catalog.createExternalTable("web_page", s"hdfs://${namenode}:9000/home/$wd/work/tpcds$scale/web_page", format)
spark.catalog.createExternalTable("web_site", s"hdfs://${namenode}:9000/home/$wd/work/tpcds$scale/web_site", format)
spark.catalog.createExternalTable("store_sales", s"hdfs://${namenode}:9000/home/$wd/work/tpcds$scale/store_sales", format)

spark.catalog.createExternalTable("catalog_returns", s"hdfs://${namenode}:9000/home/$wd/work/tpcds$scale/catalog_returns", format)
spark.catalog.createExternalTable("catalog_sales", s"hdfs://${namenode}:9000/home/$wd/work/tpcds$scale/catalog_sales", format)
spark.catalog.createExternalTable("store_returns", s"hdfs://${namenode}:9000/home/$wd/work/tpcds$scale/store_returns", format)
spark.catalog.createExternalTable("web_returns", s"hdfs://${namenode}:9000/home/$wd/work/tpcds$scale/web_returns", format)
spark.catalog.createExternalTable("web_sales", s"hdfs://${namenode}:9000/home/$wd/work/tpcds$scale/web_sales", format)


:q
