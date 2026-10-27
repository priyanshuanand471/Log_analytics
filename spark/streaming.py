"""Speed path: Kafka -> Spark -> HDFS raw parquet + Cassandra serving tables."""
import os,time
from pyspark.sql import functions as F
from common import spark_session,parse_json,cassandra

spark=spark_session('SpeedLayer-LogAnalytics')
spark.sparkContext.setLogLevel('WARN')
hdfs=os.getenv('HDFS_URI','hdfs://namenode:9000')
source=(spark.readStream.format('kafka')
    .option('kafka.bootstrap.servers',os.getenv('KAFKA_BOOTSTRAP','kafka:9092'))
    .option('subscribe',os.getenv('KAFKA_TOPIC','application-logs'))
    .option('startingOffsets','earliest')
    .load())
parsed=parse_json(source)

def save_partition(rows):
    # One Cassandra connection per Spark partition, not per record.
    cluster,session=cassandra()
    try:
        stmt=session.prepare('INSERT INTO events_by_service (service,day,event_time,event_id,level,message,host) VALUES (?,?,?,?,?,?,?)')
        for r in rows:
            session.execute(stmt,(r.service,r.day,r.event_time,r.event_id,r.level,r.message,r.host))
    finally:
        cluster.shutdown()

def process_batch(df,batch_id):
    if not df.take(1):
        return
    # Deterministic batch directory: retries overwrite same Parquet files.
    # HDFS is the cold-path raw archive used by the separate batch job.
    df.write.mode('overwrite').parquet(f'{hdfs}/data/logs/batch_id={batch_id}')
    (df.withColumn('day',F.to_date('event_time'))
       .select('service','day','event_time','event_id','level','message','host')
       .foreachPartition(save_partition))
    counts=(df.groupBy('service','level').count().collect())
    cluster,session=cassandra()
    try:
        stmt=session.prepare('INSERT INTO metrics_by_window (bucket,service,level,count) VALUES (?,?,?,?)')
        # Bucket per micro-batch is for demonstration, not an event-time window.
        bucket=f'batch-{batch_id:08d}'
        for row in counts:
            session.execute(stmt,(bucket,row.service,row.level,int(row['count'])))
    finally:
        cluster.shutdown()
    print(f'Processed batch {batch_id} with {df.count()} events',flush=True)

query=(parsed.writeStream.foreachBatch(process_batch)
       .option('checkpointLocation',f'{hdfs}/checkpoints/speed')
       .trigger(processingTime='15 seconds').start())
query.awaitTermination()
