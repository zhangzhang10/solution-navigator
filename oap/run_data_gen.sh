export SPARK_HOME=full_path_to_spark
cat ./data_gen_3t.scala | $SPARK_HOME/bin/spark-shell  \
	--num-executors 6 \
	--driver-memory 20g \
	--executor-memory 20g \
	--executor-cores  8 \
	--master yarn \
	--deploy-mode client \
