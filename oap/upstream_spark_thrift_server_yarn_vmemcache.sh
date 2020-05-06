SPARK_HOME=path_to_spark2.3.2
THRIFTSERVER_START_CMD="${SPARK_HOME}/sbin/start-thriftserver.sh"
THRIFTSERVER_STOP_CMD="${SPARK_HOME}/sbin/stop-thriftserver.sh"

THRIFTSERVER_CONFIG="--num-executors 2 \
	--driver-memory 20g \
	--executor-memory 40g \
	--executor-cores 56 \
	--master yarn \
	--deploy-mode client \
	--conf spark.speculation=false \
	--conf spark.kryoserializer.buffer.max=256m \
	--conf spark.kryoserializer.buffer=64m \
	--conf spark.sql.oap.rowgroup.size=972800 \
	--conf spark.sql.inMemoryColumnarStorage.compressed=true \
	--conf spark.sql.autoBroadcastJoinThreshold=31457280 \
	--conf spark.sql.broadcastTimeout=3600 \
	--hiveconf hive.server2.thrift.port=10001 \
	--hiveconf hive.server2.thrift.bind.host=hostname-master"

start(){
  $THRIFTSERVER_START_CMD $THRIFTSERVER_CONFIG
}

stop(){
  $THRIFTSERVER_STOP_CMD $THRIFTSERVER_CONFIG
}

if [ $1 == "stop" ] ; then
   stop
else
   if [ $1 == "start" ] ; then
   start
   fi
fi
