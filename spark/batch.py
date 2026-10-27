"""Cold path: read HDFS parquet archive and store historical aggregates."""
import os
from pyspark.sql import functions as F
from common import spark_session,cassandra

spark=spark_session('ColdLayer-LogAnalytics')
hdfs=os.getenv('HDFS_URI','hdfs://namenode:9000')
# Spark glob reads all batch directories written by the speed path.
logs=spark.read.parquet(f'{hdfs}/data/logs/batch_id=*')
summary=(logs.withColumn('day',F.date_format('event_time','yyyy-MM-dd'))
    .groupBy('day','service','level').count())
cluster,session=cassandra()
try:
    stmt=session.prepare('INSERT INTO batch_summary (day,service,level,count) VALUES (?,?,?,?)')
    for row in summary.collect():
        session.execute(stmt,(row.day,row.service,row.level,int(row['count'])))
finally:
    cluster.shutdown()
print('Historical batch summary updated')
spark.stop()
